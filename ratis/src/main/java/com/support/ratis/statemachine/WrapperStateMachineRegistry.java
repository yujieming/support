package com.support.ratis.statemachine;

import com.support.ratis.conf.StateMachineConfigKeys;
import com.support.ratis.conf.StateMachineProperties;

public class WrapperStateMachineRegistry<T> extends BaseStateMachineRegistry {

    private T value;

    public WrapperStateMachineRegistry(T value) {
        this.value = value;
    }

    @Override
    protected void preConfig(StateMachineProperties stateMachineProperties) {
        StateMachineConfigKeys.setWrapper(stateMachineProperties, value);
    }
}
