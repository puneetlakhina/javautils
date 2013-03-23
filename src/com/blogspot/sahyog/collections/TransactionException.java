package com.blogspot.sahyog.collections;

public class TransactionException extends RuntimeException {

    public TransactionException() {
        super();
    }

    public TransactionException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransactionException(String message) {
        super(message);
    }

    public TransactionException(Throwable cause) {
        super(cause);
    }

    /**
     *
     */
    private static final long serialVersionUID = 1598976036230570683L;

}
