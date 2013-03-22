package com.blogspot.sahyog.collections;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This is an attempt at a map with simple transaction capabilities. <br />
 *
 * This map should follow the semantics that anything within a transaction is
 * visible to consumers within the same transaction but is not visible outside
 * of the transaction until the commit happens. Any access within the same
 * thread is considered within the same transaction .
 *
 * <b>Any thread other than the transaction thread will block on any write
 * operations.</b>
 *
 * Commit needs to be atomic.
 *
 * @author puneet
 *
 */
public class TransactionalMap<K, V> implements Map<K, V> {
    private Map<K, V> transactionTransientMap = new HashMap<K, V>(); //this map is accessed by a single thread only
    private Set<K> newKeys = new HashSet<K>();
    private Set<Object> transactionTransientRemovedKeys = new HashSet<Object>();
    private Map<K, V> mainMap = new ConcurrentHashMap<K, V>();
    private static final AtomicInteger uniqueId = new AtomicInteger(0);
    private volatile Integer currentTransactionId = null;
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private static final ThreadLocal<Integer> uniqueNum = new ThreadLocal<Integer>() {
        @Override
        protected Integer initialValue() {
            return uniqueId.getAndIncrement();
        }
    };

    // Transaction related methods
    public synchronized void begin() {
        if (currentTransactionId == null) {
            rwLock.writeLock().lock();
            currentTransactionId = uniqueNum.get();
        } else if (!inTransaction()) {
            throw new RuntimeException(
                    "transaction map can only handle one transaction at a time. There is no concurrency because there is no serializability");
        } else {
            throw new RuntimeException("Begin should only be called once");
        }

    }

    public void commit() {
        ensureInTransaction();
        for (Object k : transactionTransientRemovedKeys) {
            mainMap.remove(k);
        }
        mainMap.putAll(transactionTransientMap);
        transactionTransientMap.clear();
        transactionTransientRemovedKeys.clear();
        rwLock.writeLock().unlock();
    }

    public void abort() {
        try {
            ensureInTransaction();
            transactionTransientMap.clear();
            transactionTransientRemovedKeys.clear();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public int size() {
        if (inTransaction()) {
            return newKeys.size() + mainMap.size() - transactionTransientRemovedKeys.size();
        } else {
            return mainMap.size();
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        if (inTransaction()) {
            return !transactionTransientRemovedKeys.contains(key) && (mainMap.containsKey(key) || transactionTransientMap.containsKey(key));
        } else {
            return mainMap.containsKey(key);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public V get(Object key) {
        if (inTransaction()) {
            if (transactionTransientRemovedKeys.contains(key)) {
                return null;
            }
            V transientMapValue = transactionTransientMap.get(key);
            return (transientMapValue != null) ? transientMapValue : mainMap.get(key);
        } else {
            return mainMap.get(key);
        }
    }

    @Override
    public V put(K key, V value) {
        if (inTransaction()) {
            return putInTransaction(key, value);
        } else {
            rwLock.writeLock().lock();
            return putNoTransaction(key, value);
        }
    }

    private V putInTransaction(K key, V value) {
        if (transactionTransientRemovedKeys.contains(key)) {
            transactionTransientRemovedKeys.remove(key);
        }
        if (!newKeys.contains(key) && !mainMap.containsKey(key)) {
            newKeys.add(key);
        }
        V transientMapPreviousValue = transactionTransientMap.put(key, value);
        return (transientMapPreviousValue != null) ? transientMapPreviousValue : mainMap.get(key);
    }

    private V putNoTransaction(K key, V value) {
        rwLock.writeLock().lock();
        try {
            return mainMap.put(key, value);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public V remove(Object key) {
        if (inTransaction()) {
            transactionTransientRemovedKeys.add(key);
            newKeys.remove(key);
            V transientMapPreviousValue = transactionTransientMap.remove(key);
            return (transientMapPreviousValue != null) ? transientMapPreviousValue : mainMap.get(key);
        } else {
            rwLock.writeLock().lock();
            try {
                return mainMap.remove(key);
            } finally {
                rwLock.writeLock().unlock();
            }
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        if (!inTransaction()) {
            rwLock.writeLock().lock();
            try {
                mainMap.putAll(m);
            } finally {
                rwLock.writeLock().unlock();
            }
            return;
        } else {
            for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
                putInTransaction(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void clear() {
        if (inTransaction()) {
            transactionTransientRemovedKeys.clear();
            transactionTransientRemovedKeys.addAll(mainMap.keySet());
            transactionTransientMap.clear();
        } else {
            rwLock.writeLock().lock();
            try {
                mainMap.clear();
            } finally {
                rwLock.writeLock().unlock();
            }
        }
    }

    @Override
    public Set<K> keySet() {
        if(inTransaction()) {
            return null; //TODO
        } else {
            rwLock.writeLock().lock(); //keySet() is a write operation since any modification of the returnned set are supposed to reflect back in the map
            try {
                return mainMap.keySet();
            } finally {
                rwLock.writeLock().unlock();
            }
        }

    }

    @Override
    public Collection<V> values() {
        if(inTransaction()) {
            return null; //TODO
        } else {
            rwLock.writeLock().lock(); //values() is a write operation since any modification of the returnned set are supposed to reflect back in the map
            try {
                return mainMap.values();
            } finally {
                rwLock.writeLock().unlock();
            }
        }
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        if(inTransaction()) {
            return null; //TODO
        } else {
            rwLock.writeLock().lock(); //entrySet() is a write operation since any modification of the returnned set are supposed to reflect back in the map
            try {
                return mainMap.entrySet();
            } finally {
                rwLock.writeLock().unlock();
            }
        }
    }

    private boolean inTransaction() {
        return currentTransactionId == uniqueNum.get();
    }

    /**
     * Ensure we are in a transaction. other wise throw exception
     */
    private void ensureInTransaction() {
        if (!inTransaction()) {
            throw new RuntimeException("Commit called when not in transaction");
        }
    }

    private boolean isTransactionOnGoing() {
        return currentTransactionId != null;
    }
}
