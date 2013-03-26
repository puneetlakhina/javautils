/**
 *
 */
package com.blogspot.sahyog.collections;

/**
 * This class assists in writing tests that run threads. It allows you to
 * implement a method which throws exceptions and then exposes diagnostic fields
 * for you to check if the thread filled.
 */
public abstract class FailureDetectingThread extends Thread {
    protected boolean isFailed = false;
    protected Throwable failedException = null;

    public FailureDetectingThread() {

    }
    public FailureDetectingThread(String name) {
        super(name);
    }
    @Override
    public final void run() {
        try {
            runWrapped();
        } catch (Throwable e) {
            isFailed = true;
            failedException = e;
        }
    }
    /**
     * Subclass should implement this method to implement what gets run.
     * @throws Exception
     */
    public abstract void runWrapped() throws Exception;
    public boolean isFailed() {
        return isFailed;
    }
    public Throwable getFailedException() {
        return failedException;
    }
}
