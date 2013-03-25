package com.blogspot.sahyog.collections;

import java.util.concurrent.Callable;

import org.junit.Test;

import org.junit.Before;

import java.util.HashMap;
import static org.junit.Assert.*;

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
        transactionalMap.put(key,value);
        ensureContainsKeyValue(key, value);
    }

    @Test
    public void transactionalSanityTest() {
        String key = "k1";
        String value = "v1";
        transactionalMap.beginTransaction();
        transactionalMap.put(key,value);
        ensureContainsKeyValue(key, value);
        transactionalMap.commit();
        ensureContainsKeyValue(key, value);
    }

    @Test
    public void abortTest() {
        String key = "k1";
        String value = "v1";
        transactionalMap.beginTransaction();
        transactionalMap.put(key,value);
        ensureContainsKeyValue(key, value);
        transactionalMap.abort();
        assertFalse(transactionalMap.containsKey(key));
        assertNull(transactionalMap.get(key));
    }

    @Test
    public void transactionAndNoTransaction() {
        String key = "k1";
        String value = "v1";
        transactionalMap.put(key,value);
        ensureContainsKeyValue(key, value);

        String transactionKey = "tk1";
        String transactionValue = "tv1";
        transactionalMap.beginTransaction();
        transactionalMap.put(transactionKey,transactionValue);
        ensureContainsKeyValue(transactionKey, transactionValue);
        transactionalMap.commit();
        ensureContainsKeyValue(transactionKey, transactionValue);
        ensureContainsKeyValue(key, value);
    }

    @Test
    public void testInterleaving() throws Exception {
        final String transactionKey = "tk1";
        final String transactionValue = "tv1";
    }

    private class InterleavedSignallingThread extends Thread {
        private boolean isFailed = false;
        private Throwable failedException = null;
        public <V> InterleavedSignallingThread(Callable<V> callable, Object lockingObject, MutableBoolean shouldRun, MutableBoolean runOther) {

        }
        @Override
        public void run() {
            try {

            } catch(Throwable e) {
                isFailed = true;
                failedException = e;
            }
        }
    }
    private void ensureContainsKeyValue(String key, String value) {
        assertTrue(transactionalMap.containsKey(key));
        assertTrue(transactionalMap.containsValue(value));
        assertEquals(value, transactionalMap.get(key));
    }
}
