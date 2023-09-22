package com.support.ratis.statemachine;

import com.support.ratis.conf.StateMachineProperties;

import java.util.function.Consumer;

public class StateMachinePropertiesConfigStateMachineRegistry extends WrapperStateMachineRegistry<Consumer<StateMachineProperties>> {

    public StateMachinePropertiesConfigStateMachineRegistry(Consumer<StateMachineProperties> value) {
        super(value);
    }

    @Override
    protected void preConfig(StateMachineProperties stateMachineProperties) {
        getValue().accept(stateMachineProperties);
    }
}
