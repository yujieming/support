
package com.support.example;


import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;

import java.io.Closeable;
import java.io.IOException;

import static com.support.example.Constants.*;

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
            .setRaftGroup(RAFT_GROUP_STORE)
            .build();


    @Override
    public void close() throws IOException {
        client.close();
    }

    public static void main(String[] args) {
        try (CreateGroupClient client = new CreateGroupClient()) {
            doCreate(client,RAFT_GROUP_COUNTER);
            doCreate(client,RAFT_GROUP_STORE);
            doCreate(client,RAFT_GROUP_COUNTER_DISPATCH);
            doCreate(client,RAFT_GROUP_STORE_DISPATCH);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }


    private static void doCreate(CreateGroupClient client, RaftGroup group) throws IOException {
        for (RaftPeer peer : PEERS) {
            do {
                try {
                    GroupInfoReply info = client.client.getGroupManagementApi(peer.getId())
                            .info(group.getGroupId());
                    System.out.println(info);
                    break;
                } catch (StatusRuntimeException e) {
                    Status status = e.getStatus();
                    if (status.getDescription().contains("not found")) {
                        client.client.getGroupManagementApi(peer.getId())
                                .add(group);
                        break;
                    }
                }
            } while (true);
        }
    }
}
