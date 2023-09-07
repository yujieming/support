package com.support.ratis.converter;


import com.support.extension.ExtensionLoader;
import com.support.ratis.statemachine.StateMachineType;
import org.apache.ratis.protocol.RaftGroupId;

import java.util.Optional;
import java.util.UUID;

public interface RaftGroupIdConverter {

    static StateMachineType convert(RaftGroupId raftGroupId) {
        ExtensionLoader<StateMachineType> stateMachineTypeExtensionLoader = ExtensionLoader.getExtensionLoader(StateMachineType.class);

        Optional<StateMachineType> stateMachineTypeOptional = stateMachineTypeExtensionLoader.getLoadedExtensions().stream().sorted().filter(type -> type.match(raftGroupId.getUuid())).findFirst();

        if (!stateMachineTypeOptional.isPresent()) {
            throw new RuntimeException("not exist");
        }
        StateMachineType stateMachineType = stateMachineTypeOptional.get();
        return stateMachineType;
    }

    static RaftGroupId reverse(StateMachineType type) {
        UUID uuid = type.generator();
        RaftGroupId raftGroupId = RaftGroupId.valueOf(uuid);
        return raftGroupId;
    }
}
