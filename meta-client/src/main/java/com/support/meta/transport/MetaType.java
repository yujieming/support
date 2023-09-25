package com.support.meta.transport;

@FunctionalInterface
public interface MetaType {

    static final MetaType DEFAULT = () -> null;

    String getType();

}
