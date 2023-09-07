package com.support.ratis.statemachine;

import com.support.extension.SPI;
import com.support.ratis.conf.StateMachineProperties;
import org.apache.ratis.statemachine.StateMachine;

@SPI
public interface StateMachineInit {

    void config(StateMachineProperties properties);

    StateMachine init();

}
