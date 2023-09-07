/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.support.counter.test;

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
  public static final List<RaftPeer> PEERS2;
  public static final List<TimeDuration> SIMULATED_SLOWNESS = null;

  static {
    final Properties properties = new Properties();
    properties.setProperty("raft.server.address.list","127.0.0.1:10024,127.0.0.1:10124,127.0.0.1:11124");
    properties.setProperty("raft.server.address.list2","127.0.0.1:10025,127.0.0.1:10125,127.0.0.1:11125");

    Function<String, String[]> parseConfList = confKey ->
        Optional.ofNullable(properties.getProperty(confKey))
        .map(s -> s.split(","))
        .orElse(null);
    final String key = "raft.server.address.list";
    final String[] addresses = parseConfList.apply(key);
    if (addresses == null || addresses.length == 0) {
      throw new IllegalArgumentException("Failed to get " + key + " from " );
    }
    final String key2 = "raft.server.address.list2";
    final String[] addresses2 = parseConfList.apply(key2);
    if (addresses2 == null || addresses2.length == 0) {
      throw new IllegalArgumentException("Failed to get " + key + " from " );
    }

    final List<RaftPeer> peers = new ArrayList<>(addresses.length);
    for (int i = 0; i < addresses.length; i++) {
      peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress(addresses[i]).build());
    }
    final List<RaftPeer> peers2 = new ArrayList<>(addresses2.length);
    for (int i = 0; i < addresses2.length; i++) {
      peers2.add(RaftPeer.newBuilder().setId("n" + i).setAddress(addresses2[i]).build());
    }
    PEERS = Collections.unmodifiableList(peers);
    PEERS2 = Collections.unmodifiableList(peers2);

    RaftPeer nt = RaftPeer.newBuilder().setId("nt").setAddress("127.0.0.1:8888").build();

    PEERNT = Collections.unmodifiableList(new ArrayList<RaftPeer>(PEERS){{
      this.add(nt);
    }});
  }

  public static final UUID GROUP_ID = UUID.fromString("00000d47-d67c-49a3-9011-abb3109a44c1");

  public static final RaftGroup RAFT_GROUP = RaftGroup.valueOf(RaftGroupId.valueOf(Constants.GROUP_ID), PEERS);

  private Constants() {
  }
}
