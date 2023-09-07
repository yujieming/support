package com.support.ratis;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.statemachine.StateMachine;

import java.util.function.Function;

@FunctionalInterface
public interface StateMachineRegistry extends Function<RaftGroupId, StateMachine> {

}
