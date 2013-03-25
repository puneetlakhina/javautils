package com.blogspot.sahyog.collections;

public class MutableBoolean {
    private boolean underlyingBoolean = false;
    public MutableBoolean() {

    }

    public void makeTrue() {
        underlyingBoolean = true;
    }

    public void makeFalse() {
        underlyingBoolean = false;
    }

    public boolean get() {
        return underlyingBoolean;
    }
}
