package com.blogspot.sahyog.collections;

public interface Transactionable {

    public void beginTransaction();
    public void commit() throws IllegalStateException;
    public void abort() throws IllegalStateException;;

}
