
package com.support.example.counter;


import com.support.counter.CounterCommand;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusException;
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
public final class CounterClient implements Closeable {
    //build the client
    private final RaftClient client = RaftClient.newBuilder()
            .setProperties(new RaftProperties())
            .setRaftGroup(RAFT_GROUP)
            .build();


    @Override
    public void close() throws IOException {
        client.close();
    }

    private void run(int increment, boolean blocking) throws Exception {

        System.out.printf("Sending %d %s command(s) using the %s ...%n",
                increment, CounterCommand.INCREMENT, blocking ? "BlockingApi" : "AsyncApi");
        final List<Future<RaftClientReply>> futures = new ArrayList<>(increment);


        //send INCREMENT command(s)
        if (blocking) {
            // use BlockingApi
            final ExecutorService executor = Executors.newFixedThreadPool(10);
            for (int i = 0; i < increment; i++) {
                final Future<RaftClientReply> f = executor.submit(
                        () -> client.io().send(CounterCommand.INCREMENT.getMessage()));
                futures.add(f);
            }
            executor.shutdown();
        } else {
            // use AsyncApi
            for (int i = 0; i < increment; i++) {
                final Future<RaftClientReply> f = client.async().send(CounterCommand.INCREMENT.getMessage());
                futures.add(f);
            }
        }

        //wait for the futures
        for (Future<RaftClientReply> f : futures) {
            final RaftClientReply reply = f.get();
            if (reply.isSuccess()) {
                final String count = reply.getMessage().getContent().toStringUtf8();
                System.out.println("Counter is incremented to " + count);
            } else {
                System.err.println("Failed " + reply);
            }
        }

        //send a GET command and print the reply
        final RaftClientReply reply = client.io().sendReadOnly(CounterCommand.GET.getMessage());
        final String count = reply.getMessage().getContent().toStringUtf8();
        System.out.println("Current counter value: " + count);

        // using Linearizable Read
        futures.clear();
        final long startTime = System.currentTimeMillis();
        final ExecutorService executor = Executors.newFixedThreadPool(PEERS.size());
        PEERS.forEach(p -> {
            final Future<RaftClientReply> f = CompletableFuture.supplyAsync(() -> {
                try {
                    return client.io().sendReadOnly(CounterCommand.GET.getMessage(), p.getId());
                } catch (IOException e) {
                    System.err.println("Failed read-only request");
                    return RaftClientReply.newBuilder().setSuccess(false).build();
                }
            }, executor).whenCompleteAsync((r, ex) -> {
                if (ex != null || !r.isSuccess()) {
                    System.err.println("Failed " + r);
                    return;
                }
                final long endTime = System.currentTimeMillis();
                final long elapsedSec = (endTime - startTime) / 1000;
                final String countValue = r.getMessage().getContent().toStringUtf8();
                System.out.println("read from " + p.getId() + " and get counter value: " + countValue
                        + ", time elapsed: " + elapsedSec + " seconds");
            });
            futures.add(f);
        });

        for (Future<RaftClientReply> f : futures) {
            f.get();
        }
    }

    public static void main(String[] args) {
        try (CounterClient client = new CounterClient()) {
            for (RaftPeer peer : PEERS) {
                do {
                    try{
                        GroupInfoReply info = client.client.getGroupManagementApi(peer.getId())
                                .info(RAFT_GROUP.getGroupId());
                        System.out.println(info);
                        break;
                    }catch (StatusRuntimeException e){
                        Status status = e.getStatus();
                        if(status.getDescription().contains("not found")){
                            client.client.getGroupManagementApi(peer.getId())
                                    .add(RAFT_GROUP);
                            break;
                        }
                    }
                } while (true);
            }
//            RaftClientReply raftClientReply = client.client.admin().setConfiguration(PEERS);

            //the number of INCREMENT commands, default is 10
            final int increment = 10;
            final boolean io = true;
            client.run(increment, io);
        } catch (Throwable e) {
            e.printStackTrace();
            System.err.println();
            System.err.println("args = " + Arrays.toString(args));
            System.err.println();
            System.err.println("Usage: java org.apache.ratis.examples.counter.client.CounterClient [increment] [async|io]");
            System.err.println();
            System.err.println("       increment: the number of INCREMENT commands to be sent (default is 10)");
            System.err.println("       async    : use the AsyncApi (default)");
            System.err.println("       io       : use the BlockingApi");
            System.exit(1);
        }
    }
}
