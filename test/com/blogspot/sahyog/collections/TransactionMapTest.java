package com.blogspot.sahyog.collections;

import static org.junit.Assert.*;

import java.util.HashMap;
import org.junit.Before;
import org.junit.Test;

public class TransactionMapTest {

    SingleThreadedTransactionableMap<String, String> transactionalMap;

    @Before
    public void setup() {
        transactionalMap = new SingleThreadedTransactionableMap<String, String>(new HashMap<String, String>());
    }

    @Test
    public void sanityTest() {
        String key = "k1";
        String value = "v1";
        transactionalMap.put(key, value);
        ensureContainsKeyValue(key, value);
    }

    @Test
    public void transactionalSanityTest() {
        String key = "k1";
        String value = "v1";
        transactionalMap.beginTransaction();
        transactionalMap.put(key, value);
        ensureContainsKeyValue(key, value);
        transactionalMap.commit();
        ensureContainsKeyValue(key, value);
    }

    @Test
    public void abortTest() {
        String key = "k1";
        String value = "v1";
        transactionalMap.beginTransaction();
        transactionalMap.put(key, value);
        ensureContainsKeyValue(key, value);
        transactionalMap.abort();
        assertFalse(transactionalMap.containsKey(key));
        assertNull(transactionalMap.get(key));
    }

    @Test
    public void transactionAndNoTransaction() {
        String key = "k1";
        String value = "v1";
        transactionalMap.put(key, value);
        ensureContainsKeyValue(key, value);

        String transactionKey = "tk1";
        String transactionValue = "tv1";
        transactionalMap.beginTransaction();
        transactionalMap.put(transactionKey, transactionValue);
        ensureContainsKeyValue(transactionKey, transactionValue);
        transactionalMap.commit();
        ensureContainsKeyValue(transactionKey, transactionValue);
        ensureContainsKeyValue(key, value);
    }

        private void ensureContainsKeyValue(String key, String value) {
        assertTrue(transactionalMap.containsKey(key));
        assertTrue(transactionalMap.containsValue(value));
        assertEquals(value, transactionalMap.get(key));
    }
}
