package com.blogspot.sahyog.collections;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
 * Commit needs to be atomic.
 * 
 * @author puneet
 * 
 */
public class TransactionalMap<K, V> implements Map<K, V> {
    private Map<K, V> transactionTransientMap = new HashMap<K, V>();
    private Set<Object> transactionTransientRemovedKeys = new HashSet<Object>();
    private Map<K, V> mainMap = new HashMap<K, V>();
    private static final AtomicInteger uniqueId = new AtomicInteger(0);
    private Integer currentTransactionId = null;
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private int inTransactionSize = 0;
    private static final ThreadLocal<Integer> uniqueNum = new ThreadLocal<Integer>() {
        @Override
        protected Integer initialValue() {
            return uniqueId.getAndIncrement();
        }
    };

    // Transaction related methods
    public synchronized void begin() {
        if (currentTransactionId == null) {
            currentTransactionId = uniqueNum.get();
        } else if (!inTransaction()) {
            throw new RuntimeException(
                    "transaction map can only handle one transaction at a time. There is no concurrency because there is no serializability");
        }

    }

    public void commit() {
        ensureInTransaction();
        rwLock.writeLock().lock();
        try {
            for (Object k : transactionTransientRemovedKeys) {
                mainMap.remove(k);
            }
            mainMap.putAll(transactionTransientMap);
            transactionTransientMap.clear();
            transactionTransientRemovedKeys.clear();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void abort() {
        ensureInTransaction();
        rwLock.writeLock().lock();
        try {
            transactionTransientMap.clear();
            transactionTransientRemovedKeys.clear();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public int size() {
        if (inTransaction()) {
            rwLock.readLock().lock();
            try {
                return inTransactionSize;
            } finally {
                rwLock.readLock().unlock();
            }
        } else {
            return mainMap.size();
        }
    }

    @Override
    public boolean isEmpty() {
        if (inTransaction()) {
            rwLock.readLock().lock();
            try {
                return inTransactionSize == 0;
            } finally {
                rwLock.readLock().unlock();
            }
        } else {
            return mainMap.isEmpty();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        if (inTransaction()) {
            rwLock.readLock().lock();
            try {
                return !transactionTransientRemovedKeys.contains(key)
                        && (mainMap.containsKey(key) || transactionTransientMap.containsKey(key));
            } finally {
                rwLock.readLock().unlock();
            }
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
            rwLock.readLock().lock();
            try {
                if (transactionTransientRemovedKeys.contains(key)) {
                    return null;
                }
                V transientMapValue = transactionTransientMap.get(key);
                return (transientMapValue != null) ? transientMapValue : mainMap.get(key);
            } finally {
                rwLock.readLock().unlock();
            }
        } else {
            return mainMap.get(key);
        }
    }

    @Override
    public V put(K key, V value) {
        if(inTransaction()) {
            rwLock.readLock().lock();
            try {
                if (transactionTransientRemovedKeys.contains(key)) {
                    transactionTransientRemovedKeys.remove(key);
                }
                V transientMapPreviousValue = transactionTransientMap.put(key, value);
                return (transientMapPreviousValue != null) ? transientMapPreviousValue : mainMap.get(key);
            } finally {
                rwLock.readLock().unlock();
            }
        } else {
            return mainMap.put(key, value);
        }
    }

    @Override
    public V remove(Object key) {
        if(inTransaction()) {
            rwLock.readLock().lock();
            try {
                transactionTransientRemovedKeys.add(key);
                V transientMapPreviousValue = transactionTransientMap.remove(key);
                return (transientMapPreviousValue != null) ? transientMapPreviousValue : mainMap.get(key);
            } finally {
                rwLock.readLock().unlock();
            }
        } else {
            return mainMap.remove(key);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        //TODO
    }

    @Override
    public void clear() {
        // TODO Auto-generated method stub

    }

    @Override
    public Set<K> keySet() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<V> values() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        // TODO Auto-generated method stub
        return null;
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
}
