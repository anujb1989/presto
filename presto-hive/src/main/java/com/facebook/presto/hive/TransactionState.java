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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TransactionState<T>
        implements TransactionListener, Transactional<T>
{
    private final ConcurrentMap<ConnectorTransactionHandle, T> transactions = new ConcurrentHashMap<>();
    private final Supplier<T> supplier;

    public TransactionState(Supplier<T> supplier)
    {
        this.supplier = requireNonNull(supplier, "supplier is null");
    }

    @Override
    public T get(ConnectorTransactionHandle transactionHandle)
    {
        return requireNonNull(transactions.get(transactionHandle), "No such transaction");
    }

    @Override
    public void begin(ConnectorTransactionHandle transactionHandle)
    {
        T previousValue = transactions.putIfAbsent(transactionHandle, supplier.get());
        checkState(previousValue == null, "Transaction already exists");
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        remove(transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        remove(transactionHandle);
    }

    private void remove(ConnectorTransactionHandle transactionHandle)
    {
        T object = transactions.remove(transactionHandle);
        checkState(object != null, "No such transaction");
    }
}
