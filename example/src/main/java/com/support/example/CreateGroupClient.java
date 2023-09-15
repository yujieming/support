
package com.support.example;


import com.support.counter.CounterCommand;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.support.example.counter.Constants.PEERS;
import static com.support.example.counter.Constants.RAFT_GROUP;

/**
 * Counter client application, this application sends specific number of
 * INCREMENT command to the Counter cluster and at the end sends a GET command
 * and print the result
 * <p>
 * Parameter to this application indicate the number of INCREMENT command, if no
 * parameter found, application use default value which is 10
 */
public final class CreateGroupClient implements Closeable {
    //build the client
    private final RaftClient client = RaftClient.newBuilder()
            .setProperties(new RaftProperties())
            .setRaftGroup(RAFT_GROUP)
            .build();


    @Override
    public void close() throws IOException {
        client.close();
    }

    public static void main(String[] args) {
        try (CreateGroupClient client = new CreateGroupClient()) {
            for (RaftPeer peer : PEERS) {
                do {
                    try {
                        GroupInfoReply info = client.client.getGroupManagementApi(peer.getId())
                                .info(RAFT_GROUP.getGroupId());
                        System.out.println(info);
                        break;
                    } catch (StatusRuntimeException e) {
                        Status status = e.getStatus();
                        if (status.getDescription().contains("not found")) {
                            client.client.getGroupManagementApi(peer.getId())
                                    .add(RAFT_GROUP);
                            break;
                        }
                    }
                } while (true);
            }
//            RaftClientReply raftClientReply = client.client.admin().setConfiguration(PEERS);

            //the number of INCREMENT commands, default is 10
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
