package com.support.meta.statemachine;

import com.support.ratis.statemachine.StateMachineType;

import java.util.UUID;

public class MetaStoreStateMachineType implements StateMachineType {

    public static final String TYPE = "meta";

    public static final String MATCH = "10000";

    @Override
    public boolean match(UUID uuid) {
        String prefix = uuid.toString().split("-")[0];
        if (prefix.startsWith(MATCH)) {
            return true;
        }
        return false;
    }

    @Override
    public UUID generator() {
        String uuid = UUID.randomUUID().toString().replaceFirst(".{5}", MATCH);
        return UUID.fromString(uuid);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Integer order() {
        return 1;
    }
}
