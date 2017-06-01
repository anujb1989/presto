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
package com.facebook.presto.spiller;

import com.facebook.presto.spi.Page;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Iterator;
import java.util.function.IntPredicate;

import static java.util.Objects.requireNonNull;

public interface PartitioningSpiller
        extends AutoCloseable
{
    /**
     * Partition page and enqueue partitioned pages to spill writers.
     * PartitioningSpillResult::isBlocked returns completed future when finished.
     */
    PartitioningSpillResult partitionAndSpill(Page page, IntPredicate spillPartitionMask);

    /**
     * Returns iterator of previously spilled Pages from given partition.
     */
    Iterator<Page> getSpilledPages(int partition);

    /**
     * Close releases/removes all underlying resources used during spilling
     * like for example all created temporary files.
     */
    @Override
    void close();

    class PartitioningSpillResult
    {
        private ListenableFuture<?> spillingFuture;
        private IntArrayList unspilledPositions;

        public PartitioningSpillResult(ListenableFuture<?> spillingFuture, IntArrayList unspilledPositions)
        {
            this.spillingFuture = requireNonNull(spillingFuture, "spillingFuture is null");
            this.unspilledPositions = requireNonNull(unspilledPositions, "unspilledPositions is null");
        }

        public ListenableFuture<?> getSpillingFuture()
        {
            return spillingFuture;
        }

        public IntArrayList getUnspilledPositions()
        {
            return unspilledPositions;
        }
    }
}
