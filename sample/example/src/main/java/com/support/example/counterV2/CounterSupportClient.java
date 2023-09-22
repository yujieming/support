
package com.support.example.counterV2;


import com.support.counter.CounterCommand;
import com.support.ratis.client.RatisClient;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
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

import static com.support.example.Constants.*;

/**
 * Counter client application, this application sends specific number of
 * INCREMENT command to the Counter cluster and at the end sends a GET command
 * and print the result
 * <p>
 * Parameter to this application indicate the number of INCREMENT command, if no
 * parameter found, application use default value which is 10
 */
public final class CounterSupportClient implements Closeable {
    //build the client
    private final RaftClient client = RaftClient.newBuilder()
            .setProperties(new RaftProperties())
            .setRaftGroup(RAFT_GROUP_COUNTER_DISPATCH)
            .build();

    private final RatisClient ratisClient = new RatisClient(client);

    @Override
    public void close() throws IOException {
        client.close();
    }

    private void run(int increment, boolean blocking) throws Exception {

        System.out.printf("Sending %d %s command(s) using the %s ...%n",
                increment, CounterCommand.INCREMENT, blocking ? "BlockingApi" : "AsyncApi");
        final List<Future<ByteString>> futures = new ArrayList<>(increment);

        //send INCREMENT command(s)
        if (blocking) {
            // use BlockingApi
            final ExecutorService executor = Executors.newFixedThreadPool(10);
            for (int i = 0; i < increment; i++) {
                final Future<ByteString> f = executor.submit(() -> ratisClient.write(write()));
                futures.add(f);
            }
            executor.shutdown();
        } else {
            // use AsyncApi
            for (int i = 0; i < increment; i++) {
//                final Future<RaftClientReply> f = client.async().send(Message.valueOf(write()));
                final Future<ByteString> f = ratisClient.writeAsync(write());
                futures.add(f);
            }
        }

        //wait for the futures
        for (Future<ByteString> f : futures) {
            final ByteString reply = f.get();
//            ByteString content = response.getContent();
            final String count = reply.toStringUtf8();
            System.out.println("Counter is incremented to " + count);
//            if (reply.isSuccess()) {
//                final CommandProtos.Reply response = CommandProtos.Reply.parseFrom(reply.getMessage().getContent());
//
//                final String count = response.getContent().toStringUtf8();
//                System.out.println("Counter is incremented to " + count);
//            } else {
//                System.err.println("Failed " + reply);
//            }
        }

        //send a GET command and print the reply
        ByteString read = ratisClient.read(read());
//        final RaftClientReply reply = client.io().sendReadOnly(Message.valueOf(read()));
        final String count = read.toStringUtf8();
        System.out.println("Current counter value: " + count);

        // using Linearizable Read
        futures.clear();
        final long startTime = System.currentTimeMillis();
        final ExecutorService executor = Executors.newFixedThreadPool(PEERS.size());
        PEERS.forEach(p -> {
            final Future<ByteString> f = CompletableFuture.supplyAsync(() -> {
                try {
                    return ratisClient.read(read(), p.getId());
                } catch (IOException e) {
                    System.err.println("Failed read-only request");
                    return ByteString.copyFromUtf8("error");
                }
            }, executor).whenCompleteAsync((r, ex) -> {
                final long endTime = System.currentTimeMillis();
                final long elapsedSec = (endTime - startTime) / 1000;
                final String countValue = r.toStringUtf8();
                System.out.println("read from " + p.getId() + " and get counter value: " + countValue
                        + ", time elapsed: " + elapsedSec + " seconds");
            });
            futures.add(f);
        });

        for (Future<ByteString> f : futures) {
            f.get();
        }
    }

    public static void main(String[] args) {
        try (CounterSupportClient client = new CounterSupportClient()) {
            for (RaftPeer peer : PEERS) {
                do {
                    try {
                        GroupInfoReply info = client.client.getGroupManagementApi(peer.getId())
                                .info(RAFT_GROUP_COUNTER_DISPATCH.getGroupId());
                        System.out.println(info);
                        break;
                    } catch (StatusRuntimeException e) {
                        Status status = e.getStatus();
                        if (status.getDescription().contains("not found")) {
                            client.client.getGroupManagementApi(peer.getId())
                                    .add(RAFT_GROUP_COUNTER_DISPATCH);
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


    private ByteString read() {
        ByteString content = CounterCommand.GET.getMessage().getContent();
        return content;
    }

    private ByteString write() {
        ByteString content = CounterCommand.INCREMENT.getMessage().getContent();
        return content;
    }
}
