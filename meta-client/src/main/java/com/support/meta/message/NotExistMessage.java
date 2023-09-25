package com.support.meta.message;

import com.support.meta.transport.MetaType;

public class NotExistMessage implements Message {


    public static final NotExistMessage NOT_EXIST = new NotExistMessage();

    @Override
    public MetaType getType() {
        return null;
    }

    private NotExistMessage() {
    }
}
