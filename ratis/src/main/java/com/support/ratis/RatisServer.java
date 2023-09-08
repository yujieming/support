package com.support.ratis;

import com.support.ratis.statemachine.BaseStateMachineRegistry;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.LifeCycle;

import java.io.Closeable;
import java.io.IOException;

public interface RatisServer extends Closeable {

    RaftServer getServer();

    BaseStateMachineRegistry getStateMachineRegistry();

    void updateStateMachineRegistry(BaseStateMachineRegistry registry);

    LifeCycle.State getLifeCycleState();

    void start() throws IOException;

}
