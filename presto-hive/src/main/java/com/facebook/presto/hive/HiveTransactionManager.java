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
package com.facebook.presto.hive;

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class HiveTransactionManager
{
    private final Set<TransactionListener> listeners;

    @Inject
    public HiveTransactionManager(Set<TransactionListener> listeners)
    {
        this.listeners = ImmutableSet.copyOf(requireNonNull(listeners, "listeners is null"));
    }

    public void begin(ConnectorTransactionHandle transactionHandle)
    {
        listeners.forEach(listener -> listener.begin(transactionHandle));
    }

    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        listeners.forEach(listener -> listener.commit(transactionHandle));
    }

    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        listeners.forEach(listener -> listener.rollback(transactionHandle));
    }
}
