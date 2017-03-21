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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

/**
 * Coordinates consumption of a resource by multiple consumers
 * in a partition-by-partition manner.
 * <p>
 * The partitions to consume can be obtained by consumers using the
 * {@code Iterable<Partition<T>> getPartitions()} method.
 * <p>
 * A {@code Partition} is only loaded after at least one consumer has called
 * {@code Partition#load()} on it and all the {@code consumersCount} consumers
 * have {@code release()}-d the previous {@code Partition} (if any).
 * <p>
 * The loaded object is {@code close()}-d when all the consumers have released
 * its {@code Partition} and thus must implement {@code Closable}.
 * <p>
 * The partitions contents are loaded using the {@code Function<Integer, CompletableFuture<T>> loader} passed
 * upon construction. The integer argument in the loader function is the number of the partition to load.
 * <p>
 * The partition number can be accessed using {@code Partition#number()} and - for a given partition -
 * it takes the value of the respective element of {@code partitionNumbers} passed upon construction.
 *
 * @param <T> type of the object loaded for each {@code Partition}. Must be {@code Closable}.
 */
public class PartitionedConsumption<T extends Closeable>
        implements Closeable
{
    private static final Logger log = Logger.get(PartitionedConsumption.class);

    private final int consumersCount;
    private final Function<Integer, ListenableFuture<T>> loader;

    // TODO FIXME Queue or something? So that we don't keep content for already released partitions

    private final List<Partition<T>> partitions;

    PartitionedConsumption(int consumersCount, Iterable<Integer> partitionNumbers, Function<Integer, ListenableFuture<T>> loader)
    {
        this(consumersCount, immediateFuture(null), partitionNumbers, loader);
    }

    PartitionedConsumption(int consumersCount, ListenableFuture<?> activator, Iterable<Integer> partitionNumbers, Function<Integer, ListenableFuture<T>> loader)
    {
        checkArgument(consumersCount > 0, "consumersCount must be positive");
        this.consumersCount = consumersCount;
        this.loader = requireNonNull(loader, "loader is null");
        this.partitions = createPartitions(activator, partitionNumbers);
    }

    private List<Partition<T>> createPartitions(ListenableFuture<?> activator, Iterable<Integer> partitionNumbers)
    {
        requireNonNull(partitionNumbers, "partitionNumbers is null");
        ImmutableList.Builder<Partition<T>> partitions = ImmutableList.builder();
        ListenableFuture<?> partitionActivator = activator;
        for (Integer partitionNumber : partitionNumbers) {
            Partition<T> partition = new Partition<>(consumersCount, partitionNumber, loader, partitionActivator);
            partitions.add(partition);
            partitionActivator = partition.released;
        }
        return partitions.build();
    }

    public int getConsumersCount()
    {
        return consumersCount;
    }

    public Iterable<Partition<T>> getPartitions()
    {
        return partitions;
    }

    @Override
    public void close()
            throws IOException
    {
        Closer closer = Closer.create();

        // TODO let's get rid of all this closing here. The class cannot handle that in any reasonable way. Really. It must be source's responsibility, we're lending resources here.
        partitions.stream()
                .map(partition -> partition.loaded)
                .forEach(future -> {
                    if (future.isDone()) {
                        // Gather already loaded so that exceptions from close are propagated, not ignored
                        Closeable result;
                        try {
                            result = getFutureValue(future);
                        }
                        catch (RuntimeException e) {
                            // must be execution exception, can ignore
                            return;
                        }
                        closer.register(result);
                    }
                    else {
                        Futures.addCallback(future, new FutureCallback<T>()
                        {
                            @Override
                            public void onSuccess(@Nullable T result)
                            {
                                try {
                                    result.close();
                                }
                                catch (IOException e) {
                                    log.error("Error closing partition", e);
                                }
                            }

                            @Override
                            public void onFailure(Throwable t)
                            {
                            }
                        });
                    }
                });

        closer.close();
    }

    public static class Partition<T extends Closeable>
    {
        private final int partitionNumber;
        private final SettableFuture<?> requested;
        private final ListenableFuture<T> loaded;
        private final SettableFuture<?> released;

        @GuardedBy("this")
        private int pendingReleases;

        public Partition(
                int consumersCount,
                int partitionNumber,
                Function<Integer, ListenableFuture<T>> loader,
                ListenableFuture<?> previousReleased)
        {
            this.partitionNumber = partitionNumber;
            this.requested = SettableFuture.create();
            this.loaded = Futures.transformAsync(
                    allAsList(requested, previousReleased),
                    ignored -> loader.apply(partitionNumber));
            this.released = SettableFuture.create();
            this.pendingReleases = consumersCount;
        }

        public int number()
        {
            return partitionNumber;
        }

        public ListenableFuture<T> load()
        {
            requested.set(null);
            return loaded;
        }

        public synchronized void release()
        {
            checkState(loaded.isDone());
            pendingReleases--;
            checkState(pendingReleases >= 0);
            if (pendingReleases == 0) {
                try {
                    getFutureValue(loaded).close();
                }
                catch (Exception e) {
                    throw new RuntimeException("Error while releasing partition", e);
                }
                released.set(null);
            }
        }
    }
}
