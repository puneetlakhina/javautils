package com.blogspot.sahyog.collections;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TransactionContext<K, V> {
    protected Map<K, V> changes = new HashMap<K, V>(); //this map is accessed by a single thread only
    protected Set<K> newKeys = new HashSet<K>();
    protected Set<Object> removedKeys = new HashSet<Object>();
    protected boolean readOnly = true;
    protected boolean cleared = false;
    protected Map<K, V> mainMap;
    private Set<Map.Entry<K, V>> entrySet;
    private Collection<V> values;
    private Set<K> keys;
    public TransactionContext(Map<K, V> mainMap) {
        this.mainMap = mainMap;
    }

    public void mergeContextIntoMainMap() {
        if (readOnly) {
            return;
        }
        if (cleared) {
            mainMap.clear();
        }
        for (Object key : removedKeys) {
            mainMap.remove(key);
        }

        for (Map.Entry<K, V> entry : changes.entrySet()) {
            mainMap.put(entry.getKey(), entry.getValue());
        }
    }

    public int size() {
        if (cleared) {
            return 0;
        }
        return mainMap.size() + newKeys.size() - removedKeys.size();
    }

    public boolean containsKey(Object key) {
        return !removedKeys.contains(key) && (changes.containsKey(key) || mainMap.containsKey(key));
    }

    /**
     * Expensive operation
     * @param value
     * @return
     */
    public boolean containsValue(Object value) {
        for(V val:values()) {
            if(value.equals(val)) {
                return true;
            }
        }
        return false;
    }

    public Set<K> keySet() {
        readOnly = false;
        if(keys == null) {
            keys = new Keys();
        }
        return keys;
    }

    public Collection<V> values() {
        readOnly = false;
        if(values == null) {
            values = new Values();
        }
        return values;
    }

    public V get(Object key) {
        if (cleared || removedKeys.contains(key)) {
            return null;
        }
        if (changes.containsKey(key)) {
            return changes.get(key);
        } else {
            return mainMap.get(key);
        }
    }

    public V put(K key, V value) {
        readOnly = false;
        cleared = false;
        removedKeys.remove(key);

        if (!mainMap.containsKey(key)) {
            newKeys.add(key);
        }
        V oldValue = null;
        if (changes.containsKey(key)) {
            oldValue = changes.put(key, value);
        } else {
            oldValue = mainMap.get(key);
            changes.put(key, value);
        }
        return oldValue;
    }

    public V remove(Object key) {
        readOnly = false;
        if (cleared) {
            return null;
        }
        removedKeys.add(key);
        newKeys.remove(key);
        V oldValue = null;
        if (changes.containsKey(key)) {
            oldValue = changes.remove(key);
        } else {
            oldValue = mainMap.get(key);
        }
        return oldValue;

    }

    public void clear() {
        readOnly = false;
        cleared = true;
        removedKeys.clear();
        changes.clear();
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new EntrySet();
        }
        return entrySet;
    }

    private final class EntrySet extends AbstractSet<Map.Entry<K, V>> {
        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        @Override
        public boolean contains(Object o) {
            if (o == null || !(o instanceof Map.Entry)) return false;
            Map.Entry<K, V> e = (Map.Entry<K, V>) o;
            return containsKey(e.getKey());
        }

        @Override
        public boolean remove(Object o) {
            return TransactionContext.this.remove(o) != null;
        }

        @Override
        public int size() {
            return TransactionContext.this.size();
        }

        @Override
        public void clear() {
            TransactionContext.this.clear();
        }
    }

    private final class Values extends AbstractCollection<V> {
        @Override
        public Iterator<V> iterator() {
            return new ValueIterator(new EntryIterator());
        }
        @Override
        public int size() {
            return TransactionContext.this.size();
        }
        @Override
        public boolean contains(Object o) {
            return containsValue(o);
        }
        @Override
        public void clear() {
            TransactionContext.this.clear();
        }
    }

    private final class Keys extends AbstractSet<K> {
        @Override
        public Iterator<K> iterator() {
            return new KeyIterator(new EntryIterator());
        }
        @Override
        public int size() {
            return TransactionContext.this.size();
        }
        @Override
        public boolean contains(Object o) {
            return containsKey(o);
        }
        @Override
        public void clear() {
            TransactionContext.this.clear();
        }
    }

    private final class EntryIterator implements Iterator<Map.Entry<K, V>> {
        private Map.Entry<K, V> current = null;
        private Map.Entry<K, V> next = null;
        private Iterator<Map.Entry<K, V>> changesIterator;
        private Iterator<Map.Entry<K, V>> mainMapIterator;

        public EntryIterator() {
            changesIterator = changes.entrySet().iterator();
            mainMapIterator = mainMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            if (next == null) {
                boolean found = false;
                while (!found && changesIterator.hasNext()) {
                    Map.Entry<K, V> entry = changesIterator.next();
                    if (removedKeys.contains(entry.getKey())) {
                        continue;
                    } else {
                        found = true;
                        next = entry;
                    }
                }
                while (!found && mainMapIterator.hasNext()) {
                    Map.Entry<K, V> entry = changesIterator.next();
                    if (removedKeys.contains(entry.getKey()) || newKeys.contains(entry.getKey())) {
                        continue;
                    } else {
                        found = true;
                        next = entry;
                    }
                }
                return found;
            } else {
                return true;
            }
        }

        @Override
        public Entry<K, V> next() {
            current = next;
            next = null;
            return current;
        }

        @Override
        public void remove() {
            if (current == null) {
                throw new IllegalStateException();
            }
        }
    };

    private final class ValueIterator implements Iterator<V> {
        private final Iterator<Map.Entry<K, V>> entryIterator;
        public ValueIterator(Iterator<Map.Entry<K, V>> entryIterator) {
            this.entryIterator = entryIterator;
        }
        @Override
        public boolean hasNext() {
            return entryIterator.hasNext();
        }

        @Override
        public V next() {
            return entryIterator.next().getValue();
        }

        @Override
        public void remove() {
            entryIterator.remove();
        }
    }

    private final class KeyIterator implements Iterator<K> {
        private final Iterator<Map.Entry<K, V>> entryIterator;
        public KeyIterator(Iterator<Map.Entry<K, V>> entryIterator) {
            this.entryIterator = entryIterator;
        }
        @Override
        public boolean hasNext() {
            return entryIterator.hasNext();
        }

        @Override
        public K next() {
            return entryIterator.next().getKey();
        }

        @Override
        public void remove() {
            entryIterator.remove();
        }
    }
}
