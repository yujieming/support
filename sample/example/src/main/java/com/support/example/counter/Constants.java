package com.support.example.counter;

import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.util.TimeDuration;

import java.util.*;
import java.util.function.Function;

/**
 * Constants across servers and clients
 */
public final class Constants {
    public static final List<RaftPeer> PEERS;
    public static final List<RaftPeer> PEERNT;

    public static final List<TimeDuration> SIMULATED_SLOWNESS = null;

    static {
        final Properties properties = new Properties();
        properties.setProperty("raft.server.address.list", "127.0.0.1:10024,127.0.0.1:10124,127.0.0.1:10224");

        Function<String, String[]> parseConfList = confKey ->
                Optional.ofNullable(properties.getProperty(confKey))
                        .map(s -> s.split(","))
                        .orElse(null);
        final String key = "raft.server.address.list";
        final String[] addresses = parseConfList.apply(key);
        if (addresses == null || addresses.length == 0) {
            throw new IllegalArgumentException("Failed to get " + key + " from ");
        }

        final List<RaftPeer> peers = new ArrayList<>(addresses.length);
        for (int i = 0; i < addresses.length; i++) {
            peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress(addresses[i]).build());
        }

        PEERS = Collections.unmodifiableList(peers);

        RaftPeer nt = RaftPeer.newBuilder().setId("nt").setAddress("127.0.0.1:8888").build();

        PEERNT = Collections.unmodifiableList(new ArrayList<RaftPeer>(PEERS) {{
            this.add(nt);
        }});
    }

    public static final UUID GROUP_ID = UUID.fromString("00000d47-d67c-49a3-9011-abb3109a44c1");

    public static final UUID GROUP_ID_STORE = UUID.fromString("11111d47-d67c-49a3-9011-abb3109a44c1");

    public static final RaftGroup RAFT_GROUP = RaftGroup.valueOf(RaftGroupId.valueOf(Constants.GROUP_ID), PEERS);

    public static final RaftGroup RAFT_GROUP_COUNTER = RaftGroup.valueOf(RaftGroupId.valueOf(Constants.GROUP_ID), PEERS);

    public static final RaftGroup RAFT_GROUP_STORE = RaftGroup.valueOf(RaftGroupId.valueOf(Constants.GROUP_ID_STORE), PEERS);

    private Constants() {
    }
}
