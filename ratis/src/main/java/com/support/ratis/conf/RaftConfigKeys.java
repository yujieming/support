package com.support.ratis.conf;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static org.apache.ratis.conf.ConfUtils.*;

public interface RaftConfigKeys {
    Logger LOG = LoggerFactory.getLogger(RaftConfigKeys.class);

    static Consumer<String> getDefaultLog() {
        return LOG::info;
    }

    String PREFIX = "support.raft.server";

    String SERVER_ID = PREFIX + ".server.id";

    String SERVER_ID_DEFAULT = "local";

    static RaftPeerId raftPeerId(RaftProperties properties) {
        String serverId = get(properties::get, SERVER_ID, SERVER_ID_DEFAULT, getDefaultLog());
        return RaftPeerId.valueOf(serverId);
    }

    static void setRaftPeerId(RaftProperties properties, RaftPeerId raftPeerId) {
        set(properties::set, SERVER_ID, raftPeerId.toString());
    }
}
