package com.support.ratis;

import com.support.ratis.conf.RaftConfigKeys;
import com.support.ratis.statemachine.BaseStateMachineRegistry;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class BaseServer implements RatisServer, Function<RaftGroupId, StateMachine> {

    private final RaftServer server;

    private final LifeCycle lifeCycle;

    private volatile AtomicReference<BaseStateMachineRegistry> registry = new AtomicReference<>();


    /**
     * need {@link RaftConfigKeys#setRaftPeerId(RaftProperties, RaftPeerId)}
     *
     * @param raftProperties
     * @throws IOException
     */
    public BaseServer(RaftProperties raftProperties) throws IOException {
        this(raftProperties, null, null);
    }

    public BaseServer(RaftProperties raftProperties, RaftGroup raftGroup) throws IOException {
        this(raftProperties, null, raftGroup);
    }

    public BaseServer(RaftProperties raftProperties, BaseStateMachineRegistry registry, RaftGroup raftGroup) throws IOException {
        this.lifeCycle = new LifeCycle(UUID.randomUUID() + "-" + JavaUtils.getClassSimpleName(getClass()));
        if (Objects.isNull(registry)) {
            this.registry.set(BaseStateMachineRegistry.DEFAULT_STATEMACHINE_REGISTRY);
        } else {
            this.registry.set(registry);
        }
        RaftServer.Builder builder = RaftServer.newBuilder();
        if (Objects.nonNull(raftGroup)) {
            builder.setGroup(raftGroup);
        }
        RaftPeerId raftPeerId = RaftConfigKeys.raftPeerId(raftProperties);
        builder.setServerId(raftPeerId);
        this.server = builder.setProperties(raftProperties)
                .setStateMachineRegistry(this::apply)
                .build();
    }

    public BaseServer(RaftProperties raftProperties, BaseStateMachineRegistry registry) throws IOException {
        this(raftProperties, registry, null);
    }

    @Override
    public RaftServer getServer() {
        return this.server;
    }

    @Override
    public BaseStateMachineRegistry getStateMachineRegistry() {
        return registry.get();
    }

    @Override
    public void updateStateMachineRegistry(BaseStateMachineRegistry registry) {
        this.registry.set(registry);
    }

    @Override
    public LifeCycle.State getLifeCycleState() {
        return lifeCycle.getCurrentState();
    }

    @Override
    public void start() throws IOException {
        getLifeCycle().startAndTransition(this::startImpl, IOException.class);
    }

    private void startImpl() throws IOException {
        this.server.start();
    }

    private LifeCycle getLifeCycle() {
        return lifeCycle;
    }

    @Override
    public void close() throws IOException {
        this.server.close();
    }

    @Override
    public StateMachine apply(RaftGroupId groupId) {
        BaseStateMachineRegistry stateMachineRegistry = getStateMachineRegistry();
        if (Objects.isNull(stateMachineRegistry)) {
            throw new RuntimeException("stateMachineRegistry is null");
        }
        return stateMachineRegistry.apply(groupId);
    }
}
