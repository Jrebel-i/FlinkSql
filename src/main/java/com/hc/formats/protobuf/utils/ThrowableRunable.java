package com.hc.formats.protobuf.utils;

@FunctionalInterface
public interface ThrowableRunable<EXCEPTION extends Throwable> {

    void run() throws EXCEPTION;

}
