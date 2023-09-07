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

import com.support.ratis.BaseStateMachineRegistry;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.TimeDuration;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;

public final class CounterServer implements Closeable {
  private final RaftServer server;

  public CounterServer(RaftPeer peer, File storageDir, TimeDuration simulatedSlowness, RaftGroup group) throws IOException {
    //create a property object
    final RaftProperties properties = new RaftProperties();

    //set the storage directory (different for each peer) in the RaftProperty object
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    //set the read policy to Linearizable Read.
    //the Default policy will route read-only requests to leader and directly query leader statemachine.
    //Linearizable Read allows to route read-only requests to any group member
    //and uses ReadIndex to guarantee strong consistency.
    RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    //set the linearizable read timeout
    RaftServerConfigKeys.Read.setTimeout(properties, TimeDuration.ONE_MINUTE);

    //set the port (different for each peer) in RaftProperty object
    final int port = NetUtils.createSocketAddr(peer.getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);

    BaseStateMachineRegistry baseStateMachineRegistry = new BaseStateMachineRegistry();
    //create the counter state machine which holds the counter value
    //build the Raft server
    this.server = RaftServer.newBuilder()
        .setGroup(group)
        .setProperties(properties)
        .setServerId(peer.getId())
        .setStateMachineRegistry(baseStateMachineRegistry::apply)
        .build();
  }

  public void start() throws IOException {
    server.start();
  }

  @Override
  public void close() throws IOException {
    server.close();
  }
}
