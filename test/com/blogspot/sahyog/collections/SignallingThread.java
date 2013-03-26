/**
 *
 */
package com.blogspot.sahyog.collections;

import java.util.Iterator;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * This thread class assists in testing situations where you want to test how
 * things behave based on different interleavings of 2 threads. The way it works
 * is this: the main run method iterates over all the callables and waits for
 * someone to notify it to run and set the runMe boolean to true. Once a
 * callable finishes it sets the runMe to false and runOther to true and
 * notifies so that the other thread can run The runMe for this guy would be the
 * runOther for some one else
 */
public class SignallingThread extends FailureDetectingThread {
    private final Object signallingObject;
    private final List<Callable<Void>> callables;
    private final MutableBoolean runMe;
    private final MutableBoolean runOther;

    public SignallingThread(Object signallingObject, List<Callable<Void>> callables, MutableBoolean runMe, MutableBoolean runOther, String name) {
        super(name);
        this.signallingObject = signallingObject;
        this.callables = callables;
        this.runMe = runMe;
        this.runOther = runOther;
    }
    @Override
    public void runWrapped() throws Exception {
        Iterator<Callable<Void>> callablesIterator = callables.iterator();
        while (callablesIterator.hasNext()) {
            Callable<Void> callable = callablesIterator.next();
            synchronized (signallingObject) {
                try {
                    while (!runMe.get()) {
                        signallingObject.wait();
                    }
                    callable.call();
                    runOther.makeTrue();
                    runMe.makeFalse();
                    signallingObject.notify();
                } catch (Throwable t) {
                    t.printStackTrace();
                    failedException = t;
                    isFailed = true;
                    break;
                }
            }
        }
        //The folllowing is only to make sure the signalling happens as many times as the number of callables desipte any failures
        while (callablesIterator.hasNext()) {
            synchronized (signallingObject) {
                while (!runMe.get()) {
                    signallingObject.wait();
                }
                runOther.makeTrue();
                runMe.makeFalse();
                signallingObject.notify();
            }
        }
    }

}
