//package com.support.meta.statemachine;
//
//import com.support.ratis.conf.StateMachineProperties;
//import com.support.ratis.statemachine.StateMachineInit;
//import org.apache.ratis.statemachine.StateMachine;
//import org.apache.ratis.util.TimeDuration;
//
//public class CounterStateMachineInit implements StateMachineInit {
//
//    @Override
//    public void config(StateMachineProperties properties) {
//    }
//
//    @Override
//    public StateMachine init() {
//        return new CounterStateMachine(TimeDuration.ZERO);
//    }
//}
