package com.support.counter;

import com.support.ratis.conf.StateMachineProperties;
import com.support.ratis.statemachine.StateMachineInit;
import org.apache.ratis.statemachine.StateMachine;

public class CounterDispatchStateMachineInit implements StateMachineInit {

    @Override
    public void config(StateMachineProperties properties) {
    }

    @Override
    public StateMachine init() {
        return new CounterDispatchStateMachine();
    }
}
