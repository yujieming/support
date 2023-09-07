package com.support.ratis.statemachine;

import com.support.ratis.conf.StateMachineProperties;
import org.apache.ratis.statemachine.StateMachine;

import java.util.function.Function;

@FunctionalInterface
public interface StateMachineFactory extends Function<StateMachineProperties, StateMachineInit> {
    default StateMachine create(StateMachineProperties properties){
        StateMachineInit stateMachineInit = apply(properties);
        stateMachineInit.config(properties);
        return stateMachineInit.init();
    }
}
