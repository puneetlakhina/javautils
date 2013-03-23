package com.blogspot.sahyog.collections;

import java.util.AbstractMap;

import java.util.Collections;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * This is an attempt at a map with simple transaction capabilities. <br />
 *
 * This map should follow the semantics that anything within a transaction is
 * visible to consumers within the same transaction but is not visible outside
 * of the transaction until the commit happens. Any access within the same
 * thread is considered within the same transaction . The need here is that we
 * should be able to modify a map but not have its results available until the
 * map is committed. This implementation doesnt allow concurrent transactions
 * and forces all non transaction threads to access the map in a read only
 * fashion. It doesnt guard against change to the values mapped to existing
 * keys, but it prevents against structural modifcations of the map.
 *
 * During commit other readers might be able to see values being comitted.
 *
 * @author puneet
 *
 */
public class SingleThreadedTransactionableMap<K, V> extends AbstractMap<K, V> implements Map<K, V>, Transactionable {
    protected static ThreadLocal<TransactionContext> txContextThreadLocal = new ThreadLocal<TransactionContext>();
    private volatile boolean ongoingTransaction = false;
    private final Map<K, V> wrappedMap;

    public SingleThreadedTransactionableMap(Map<K, V> mapToWrap) {
        this.wrappedMap = mapToWrap;
    }

    @Override
    public synchronized void beginTransaction() {
        TransactionContext<K, V> existingContext = getTransactionContext();
        if (existingContext == null) {
            setupTransactionContext();
            return;
        } else {
            throw new TransactionException(
                    "An existing transaction is in progress. This implementation does not allow for concurrent transactions");
        }
    }

    @Override
    public void commit() throws IllegalStateException {
        TransactionContext<K, V> existingContext = getTransactionContext();
        if (existingContext == null) {
            throw new TransactionException("No active transaction.");
        }
        existingContext.mergeContextIntoMainMap();
        clearTransactionContext();
    }

    @Override
    public void abort() throws IllegalStateException {
        TransactionContext<K, V> existingContext = getTransactionContext();
        if (existingContext == null) {
            throw new TransactionException("No active transaction.");
        }
        clearTransactionContext();

    }

    @Override
    public int size() {
        TransactionContext<K, V> existingContext = getTransactionContext();
        return existingContext == null ? wrappedMap.size() : existingContext.size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        TransactionContext<K, V> existingContext = getTransactionContext();
        return existingContext == null ? wrappedMap.containsKey(key) : existingContext.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        TransactionContext<K, V> existingContext = getTransactionContext();
        return existingContext == null ? wrappedMap.containsValue(value) : existingContext.containsValue(value);
    }

    @Override
    public V get(Object key) {
        TransactionContext<K, V> existingContext = getTransactionContext();
        return existingContext == null ? wrappedMap.get(key) : existingContext.get(key);
    }

    @Override
    @WriteOperation
    public V put(K key, V value) {
        TransactionContext<K, V> existingContext = getTransactionContext();
        if (existingContext == null) {
            failIfOnGoingTransaction();
            return wrappedMap.put(key, value);
        } else {
            return existingContext.put(key, value);
        }
    }

    @Override
    @WriteOperation
    public V remove(Object key) {
        TransactionContext<K, V> existingContext = getTransactionContext();
        if (existingContext == null) {
            failIfOnGoingTransaction();
            return wrappedMap.remove(key);
        } else {
            return existingContext.remove(key);
        }
    }

    @Override
    @WriteOperation
    public void putAll(Map<? extends K, ? extends V> m) {
        if (m == null || m.isEmpty()) {
            return;
        }
        TransactionContext<K, V> existingContext = getTransactionContext();
        if (existingContext == null) {
            failIfOnGoingTransaction();
            wrappedMap.putAll(m);
        } else {
            for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
                existingContext.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    @WriteOperation
    public void clear() {
        TransactionContext<K, V> existingContext = getTransactionContext();
        if (existingContext == null) {
            failIfOnGoingTransaction();
            wrappedMap.clear();
        } else {
            existingContext.clear();
        }
    }

    @Override
    @WriteOperation
    /**
     * Keyset method from java util Map has the contract that any modifications are reflected back in the map. We cant have that while a transaction is on going. <br />
     * At the same time most users of this method dont actually modify any keys but just iterate.
     * So this implementation tries to give the best of both worlds. In case of an ongoing transaction it returns an unmodifiable set so that you cant do anything to it.
     */
    public Set<K> keySet() {
        TransactionContext<K, V> existingContext = getTransactionContext();
        if (existingContext == null) {
            if (ongoingTransaction) {
                return Collections.unmodifiableSet(wrappedMap.keySet());
            } else {
                return wrappedMap.keySet();
            }
        } else {
            return existingContext.keySet();
        }
    }

    @Override
    @WriteOperation
    /**
     * Behavios is similar to keyset. unmodifiable collection is returned if a transaction is currently on going.
     */
    public Collection<V> values() {
        TransactionContext<K, V> existingContext = getTransactionContext();
        if (existingContext == null) {
            if (ongoingTransaction) {
                return Collections.unmodifiableCollection(wrappedMap.values());
            } else {
                return wrappedMap.values();
            }
        } else {
            return existingContext.values();
        }
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        TransactionContext<K, V> existingContext = getTransactionContext();
        if (existingContext == null) {
            if (ongoingTransaction) {
                return Collections.unmodifiableSet(wrappedMap.entrySet());
            } else {
                return wrappedMap.entrySet();
            }
        } else {
            return existingContext.entrySet();
        }
    }

    private TransactionContext<K, V> getTransactionContext() {
        return txContextThreadLocal.get();
    }

    private void setupTransactionContext() {
        txContextThreadLocal.set(new TransactionContext<K, V>(wrappedMap));
        ongoingTransaction = true;
    }

    private void clearTransactionContext() {
        txContextThreadLocal.set(null);
        ongoingTransaction = false;
    }

    private void failIfOnGoingTransaction() {
        if (ongoingTransaction) {
            throw new IllegalStateException("A transaction is on going . No write operations are allowed");
        }
    }
}
