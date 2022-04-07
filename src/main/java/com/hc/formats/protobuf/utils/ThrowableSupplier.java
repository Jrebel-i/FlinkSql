package com.hc.formats.protobuf.utils;

@FunctionalInterface
public interface ThrowableSupplier<OUT, EXCEPTION extends Throwable> {

    OUT get() throws EXCEPTION;

}
