/**
 *
 */
package com.blogspot.sahyog.collections;

import static org.junit.Assert.*;

import java.util.HashMap;
import org.junit.Before;

import java.util.Arrays;
import java.util.concurrent.Callable;
import org.junit.Test;

/**
 * This tests the interleaving of a transaction and a non transaction thread in
 * various ways. Please note the number of callables for both threads needs to
 * be balanced.
 */
public class TransactionMapInterleavedThreadsTest {
    SingleThreadedTransactionableMap<String, String> transactionalMap;
    String transactionKey = "tk1";
    String transactionValue = "tv1";
    Callable<Void> beginTransactionAndPutCallable;
    Callable<Void> commitTransactionCallable;
    Callable<Void> abortTransactionCallable;
    Callable<Void> checkKeyDoesNotExistCallable;
    Callable<Void> checkKeyExistsCallable;
    Callable<Void> doNothingCallable;
    Object signallingObject;
    MutableBoolean runTransactionThread;
    MutableBoolean runNonTransactionThread;

    @Before
    public void setup() {
        transactionalMap = new SingleThreadedTransactionableMap<String, String>(new HashMap<String, String>());
        signallingObject = new Object();
        runTransactionThread = new MutableBoolean();
        runNonTransactionThread = new MutableBoolean();
        runNonTransactionThread.makeFalse();
        runTransactionThread.makeFalse();
    }

    @Before
    public void createCallables() {
        beginTransactionAndPutCallable = new Callable<Void>() {
            public Void call() {
                transactionalMap.beginTransaction();
                transactionalMap.put(transactionKey, transactionValue);
                return null;
            }
        };
        commitTransactionCallable = new Callable<Void>() {
            public Void call() {
                transactionalMap.commit();
                return null;
            }
        };
        abortTransactionCallable = new Callable<Void>() {
            public Void call() {
                transactionalMap.abort();
                return null;
            }
        };
        checkKeyDoesNotExistCallable = new Callable<Void>() {
            public Void call() {
                assertNull("Non transaction thread shouldnt see the value yet", transactionalMap.get(transactionKey));
                return null;
            }
        };
        checkKeyExistsCallable = new Callable<Void>() {
            public Void call() {
                assertNotNull("Non transaction thread should see the value", transactionalMap.get(transactionKey));
                return null;
            }
        };
        doNothingCallable = new Callable<Void>() {
            public Void call() {
                return null;
            }
        };
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInterleavingWithCommit() throws Exception {
        SignallingThread transactionThread = new SignallingThread(signallingObject, Arrays.asList(beginTransactionAndPutCallable,
                commitTransactionCallable), runTransactionThread, runNonTransactionThread, "transactionThread");
        SignallingThread nonTransactionThread = new SignallingThread(signallingObject, Arrays.asList(checkKeyDoesNotExistCallable,
                checkKeyExistsCallable), runNonTransactionThread, runTransactionThread, "nonTransactionThread");

        transactionThread.start();
        nonTransactionThread.start();
        Thread.sleep(100);

        notifyRunnerFor(runTransactionThread);

        transactionThread.join();
        nonTransactionThread.join();

        ensureThreadSucceeded(transactionThread);
        ensureThreadSucceeded(nonTransactionThread);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInterleavingWithAbort() throws Exception {
        SignallingThread transactionThread = new SignallingThread(signallingObject, Arrays.asList(beginTransactionAndPutCallable,
                abortTransactionCallable), runTransactionThread, runNonTransactionThread, "transactionThread");
        SignallingThread nonTransactionThread = new SignallingThread(signallingObject, Arrays.asList(checkKeyDoesNotExistCallable,
                checkKeyDoesNotExistCallable), runNonTransactionThread, runTransactionThread, "nonTransactionThread");

        transactionThread.start();
        nonTransactionThread.start();
        Thread.sleep(100); //Give a chance for both threads to reach their waiting for something stage

        notifyRunnerFor(runTransactionThread);

        transactionThread.join();
        nonTransactionThread.join();

        ensureThreadSucceeded(transactionThread);
        ensureThreadSucceeded(nonTransactionThread);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNonTransactionThreadIsReadOnly() throws Exception {
        Callable<Void> putSomethingCallable = new Callable<Void>() {
            public Void call() {
                transactionalMap.put("ot1", "otv1");
                return null;
            }
        };
        SignallingThread transactionThread = new SignallingThread(signallingObject, Arrays.asList(beginTransactionAndPutCallable),
                runTransactionThread, runNonTransactionThread, "transactionThread");
        SignallingThread nonTransactionThread = new SignallingThread(signallingObject, Arrays.asList(putSomethingCallable), runNonTransactionThread,
                runTransactionThread, "nonTransactionThread");

        transactionThread.start();
        nonTransactionThread.start();
        Thread.sleep(100); //Give a chance for both threads to reach their waiting for something stage

        notifyRunnerFor(runTransactionThread);

        transactionThread.join();
        nonTransactionThread.join();

        ensureThreadSucceeded(transactionThread);
        ensureThreadFailed(nonTransactionThread);
    }
    @SuppressWarnings("unchecked")
    @Test
    public void testNonTransactionThreadIsWriteableAfterCommit() throws Exception {
        Callable<Void> putSomethingCallable = new Callable<Void>() {
            public Void call() {
                transactionalMap.put("ot1", "otv1");
                return null;
            }
        };
        SignallingThread transactionThread = new SignallingThread(signallingObject, Arrays.asList(beginTransactionAndPutCallable,
                commitTransactionCallable, doNothingCallable), runTransactionThread, runNonTransactionThread, "transactionThread");
        SignallingThread nonTransactionThread = new SignallingThread(signallingObject, Arrays.asList(doNothingCallable, doNothingCallable,
                putSomethingCallable), runNonTransactionThread, runTransactionThread, "nonTransactionThread");

        transactionThread.start();
        nonTransactionThread.start();
        Thread.sleep(100); //Give a chance for both threads to reach their waiting for something stage

        notifyRunnerFor(runTransactionThread);

        transactionThread.join();
        nonTransactionThread.join();

        ensureThreadSucceeded(transactionThread);
        ensureThreadSucceeded(nonTransactionThread);
    }
    private void ensureThreadSucceeded(FailureDetectingThread failureDetectingThread) {
        assertFalse(failureDetectingThread.getFailedException() + "", failureDetectingThread.isFailed());
    }

    private void ensureThreadFailed(FailureDetectingThread failureDetectingThread) {
        assertTrue("Thread:" + failureDetectingThread + " should have failed", failureDetectingThread.isFailed());
    }

    private void notifyRunnerFor(MutableBoolean mutableBoolean) {
        synchronized (signallingObject) {
            mutableBoolean.makeTrue();
            signallingObject.notify();
        }
    }

}
