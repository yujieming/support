package com.support.ratis.statemachine;

import com.support.extension.SPI;

import java.util.UUID;

@SPI
public interface StateMachineType extends Comparable<StateMachineType>{

    boolean match(UUID uuid);

    UUID generator();

    String getType();

    Integer order();

    default int compareTo(StateMachineType o){
        return this.order().compareTo(o.order());
    }
}
