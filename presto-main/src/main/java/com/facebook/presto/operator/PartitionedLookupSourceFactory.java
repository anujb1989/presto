/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator;

import com.facebook.presto.operator.LookupSourceProvider.LookupSourceLease;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import static com.facebook.presto.operator.OuterLookupSource.createOuterLookupSourceSupplier;
import static com.facebook.presto.operator.PartitionedLookupSource.createPartitionedLookupSourceSupplier;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public final class PartitionedLookupSourceFactory
        implements LookupSourceFactory
{
    private final List<Type> types;
    private final List<Type> outputTypes;
    private final Map<Symbol, Integer> layout;
    private final List<Type> hashChannelTypes;
    private final boolean outer;
    private final SettableFuture<?> destroyed = SettableFuture.create();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    @GuardedBy("rwLock")
    private final Supplier<LookupSource>[] partitions;

    private final SettableFuture<?> partitionsNoLongerNeeded = SettableFuture.create();

    @GuardedBy("rwLock")
    private int partitionsSet;

    @GuardedBy("rwLock")
    private SpillingInfo spillingInfo = new SpillingInfo(0, ImmutableSet.of());

    @GuardedBy("rwLock")
    private Map<Integer, SpilledLookupSourceHandle> spilledPartitions = new HashMap<>();

    @GuardedBy("rwLock")
    private TrackingLookupSourceSupplier lookupSourceSupplier;

    @GuardedBy("rwLock")
    private final List<SettableFuture<LookupSourceProvider>> lookupSourceFutures = new ArrayList<>();

    @GuardedBy("rwLock")
    private int finishedProbeOperators;

    @GuardedBy("rwLock")
    private Optional<PartitionedConsumption<LookupSource>> partitionedConsumption = Optional.empty();

    public PartitionedLookupSourceFactory(List<Type> types, List<Type> outputTypes, List<Integer> hashChannels, int partitionCount, Map<Symbol, Integer> layout, boolean outer)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        this.layout = ImmutableMap.copyOf(layout);
        this.partitions = (Supplier<LookupSource>[]) new Supplier<?>[partitionCount];
        this.outer = outer;

        hashChannelTypes = hashChannels.stream()
                .map(types::get)
                .collect(toImmutableList());
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    @Override
    public Map<Symbol, Integer> getLayout()
    {
        return layout;
    }

    @Override
    public int partitions()
    {
        return partitions.length;
    }

    @Override
    public ListenableFuture<LookupSourceProvider> createLookupSourceProvider()
    {
        rwLock.writeLock().lock();
        try {
            if (lookupSourceSupplier != null) {
                return Futures.immediateFuture(new SpillAwareLookupSourceProvider());
            }

            SettableFuture<LookupSourceProvider> lookupSourceFuture = SettableFuture.create();
            lookupSourceFutures.add(lookupSourceFuture);
            return lookupSourceFuture;
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    public ListenableFuture<?> setPartitionLookupSourceSupplier(int partitionIndex, Supplier<LookupSource> partitionLookupSource)
    {
        requireNonNull(partitionLookupSource, "partitionLookupSource is null");

        boolean completed;

        rwLock.writeLock().lock();
        try {
            if (destroyed.isDone()) {
                return immediateFuture(null);
            }

            checkState(partitions[partitionIndex] == null, "Partition already set");
            checkState(!spilledPartitions.containsKey(partitionIndex), "Partition already set as spilled");
            partitions[partitionIndex] = partitionLookupSource;
            partitionsSet++;
            completed = partitionsSet == partitions.length;
        }
        finally {
            rwLock.writeLock().unlock();
        }

        if (completed) {
            supplyLookupSources();
        }

        return partitionsNoLongerNeeded;
    }

    public void setPartitionSpilledLookupSource(int partitionIndex, SpilledLookupSourceHandle spilledLookupSourceHandle)
    {
        requireNonNull(spilledLookupSourceHandle, "spilledLookupSourceHandle is null");

        boolean completed;

        rwLock.writeLock().lock();
        try {
            if (destroyed.isDone()) {
                return;
            }

            checkState(!spilledPartitions.containsKey(partitionIndex), "Partition already set as spilled");
            spilledPartitions.put(partitionIndex, spilledLookupSourceHandle);
            spillingInfo = new SpillingInfo(spillingInfo.spillEpoch() + 1, spilledPartitions.keySet());

            if (partitions[partitionIndex] != null) {
                // Was build and now it's spilled
                completed = false;
            }
            else {
                partitionsSet++;
                completed = partitionsSet == partitions.length;
            }

            partitions[partitionIndex] = () -> new SpilledLookupSource();
        }
        finally {
            rwLock.writeLock().unlock();
        }

        if (completed) {
            supplyLookupSources();
        }
    }

    private void supplyLookupSources()
    {
        checkState(!rwLock.isWriteLockedByCurrentThread());

        List<SettableFuture<LookupSourceProvider>> lookupSourceFutures;

        rwLock.writeLock().lock();
        try {
            checkState(partitionsSet == partitions.length, "Not all set yet");
            checkState(this.lookupSourceSupplier == null, "Already supplied");

            if (partitionsSet != 1) {
                List<Supplier<LookupSource>> partitions = ImmutableList.copyOf(this.partitions);
                this.lookupSourceSupplier = createPartitionedLookupSourceSupplier(partitions, hashChannelTypes, outer);
            }
            else if (outer) {
                this.lookupSourceSupplier = createOuterLookupSourceSupplier(partitions[0]);
            }
            else {
                checkState(!spillingInfo.hasSpilled(), "Spill not supported when there is single partition");
                this.lookupSourceSupplier = TrackingLookupSourceSupplier.from(partitions[0]);
            }

            // store futures into local variables so they can be used outside of the lock
            lookupSourceFutures = ImmutableList.copyOf(this.lookupSourceFutures);
        }
        finally {
            rwLock.writeLock().unlock();
        }

        for (SettableFuture<LookupSourceProvider> lookupSourceFuture : lookupSourceFutures) {
            lookupSourceFuture.set(new SpillAwareLookupSourceProvider());
        }
    }

    @Override
    public PartitionedConsumption<LookupSource> finishProbeOperator(OptionalInt lookupJoinsCount)
    {
        rwLock.writeLock().lock();
        try {
            if (!spillingInfo.hasSpilled()) {
                finishedProbeOperators++;
                return new PartitionedConsumption<LookupSource>(1, emptyList(), i -> {
                    throw new UnsupportedOperationException();
                });
            }

            int operatorsCount = lookupJoinsCount
                    .orElseThrow(() -> new IllegalStateException("Indeterminate number of LookupJoinOperator-s when using spill to disk. This is a bug."));

            if (!partitionedConsumption.isPresent()) {
                // This is the first probe to finish after anything has been spilled.
                checkState(finishedProbeOperators <= operatorsCount);
                partitionedConsumption = Optional.of(
                        new PartitionedConsumption<>(
                                operatorsCount - finishedProbeOperators,
                                partitionsNoLongerNeeded,
                                spilledPartitions.keySet(),
                                this::loadSpilledLookupSource));
            }

            if (finishedProbeOperators + partitionedConsumption.get().getConsumersCount() == operatorsCount) {
                // Free memory
                partitionsNoLongerNeeded.set(null);
                Arrays.fill(partitions, null);
            }

            return partitionedConsumption.get();
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    private ListenableFuture<LookupSource> loadSpilledLookupSource(int partitionNumber)
    {
        SpilledLookupSourceHandle spilledLookupSourceHandle = requireNonNull(spilledPartitions.get(partitionNumber), "spilledPartitions.get(partitionNumber) is null");
        return spilledLookupSourceHandle.getLookupSource();
    }

    @Override
    public OuterPositionIterator getOuterPositionIterator()
    {
        TrackingLookupSourceSupplier lookupSourceSupplier;

        rwLock.writeLock().lock();
        try {
            checkState(this.lookupSourceSupplier != null, "lookup source not ready yet");
            lookupSourceSupplier = this.lookupSourceSupplier;
        }
        finally {
            rwLock.writeLock().unlock();
        }

        return lookupSourceSupplier.getOuterPositionIterator();
    }

    @Override
    public void destroy()
    {
        destroyed.set(null);
        partitionsNoLongerNeeded.set(null);
    }

    public ListenableFuture<?> isDestroyed()
    {
        return nonCancellationPropagating(destroyed);
    }

    private class SpillAwareLookupSourceProvider
            implements LookupSourceProvider
    {
        @Override
        public LookupSourceLease leaseLookupSource()
        {
            rwLock.readLock().lock();
            try {
                LookupSourceLease lease = new SpillAwareLookupSourceLease(
                        lookupSourceSupplier.getLookupSource(),
                        spillingInfo,
                        rwLock.readLock()::unlock);

                /*
                 * Read lock is held until lease is closed
                 */
                //noinspection LockAcquiredButNotSafelyReleased
                rwLock.readLock().lock();
                return lease;
            }
            finally {
                rwLock.readLock().unlock();
            }
        }

        @Override
        public void close()
        {
        }
    }

    private static class SpilledLookupSource
            implements LookupSource
    {
        @Override
        public int getChannelCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getInMemorySizeInBytes()
        {
            return 0;
        }

        @Override
        public int getJoinPositionCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long checksum()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
        }
    }

    private static class SpillAwareLookupSourceLease
            implements LookupSourceLease
    {
        private final LookupSource lookupSource;
        private final SpillingInfo spillingInfo;
        private final Runnable onClose;

        public SpillAwareLookupSourceLease(LookupSource lookupSource, SpillingInfo spillingInfo, Runnable onClose)
        {
            this.lookupSource = requireNonNull(lookupSource, "lookupSource is null");
            this.spillingInfo = requireNonNull(spillingInfo, "spillingInfo is null");
            this.onClose = requireNonNull(onClose, "onClose is null");
        }

        @Override
        public LookupSource getLookupSource()
        {
            return lookupSource;
        }

        @Override
        public boolean hasSpilled()
        {
            return spillingInfo.hasSpilled();
        }

        @Override
        public long spillEpoch()
        {
            return spillingInfo.spillEpoch();
        }

        @Override
        public IntPredicate getSpillMask()
        {
            return spillingInfo.getSpillMask();
        }

        @Override
        public void close()
        {
            lookupSource.close();
            onClose.run();
        }
    }

    @Immutable
    private static final class SpillingInfo
    {
        private final long spillEpoch;
        private final Set<Integer> spilledPartitions;

        SpillingInfo(long spillEpoch, Set<Integer> spilledPartitions)
        {
            this.spillEpoch = spillEpoch;
            this.spilledPartitions = ImmutableSet.copyOf(requireNonNull(spilledPartitions, "spilledPartitions is null"));
        }

        boolean hasSpilled()
        {
            return !spilledPartitions.isEmpty();
        }

        long spillEpoch()
        {
            return spillEpoch;
        }

        IntPredicate getSpillMask()
        {
            return spilledPartitions::contains;
        }
    }
}
