
package com.support.meta.run;


import com.support.meta.proto.MetaProtos.*;
import com.support.ratis.client.RatisCoverClient;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.support.meta.run.Constants.PEERS;
import static com.support.meta.run.Constants.RAFT_GROUP_META;

/**
 * Counter client application, this application sends specific number of
 * INCREMENT command to the Counter cluster and at the end sends a GET command
 * and print the result
 * <p>
 * Parameter to this application indicate the number of INCREMENT command, if no
 * parameter found, application use default value which is 10
 */
public final class ReadDispatchClient implements Closeable {
    //build the client
    private final RaftClient client = RaftClient.newBuilder()
            .setProperties(new RaftProperties())
            .setRaftGroup(RAFT_GROUP_META)
            .build();

    private final RatisCoverClient<MetaReplyProto> ratisClient = new RatisCoverClient<MetaReplyProto>(client) {
        @Override
        public MetaReplyProto cover(ByteString content) {
            try {
                return MetaReplyProto.parseFrom(content);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    };


    @Override
    public void close() throws IOException {
        client.close();
    }

    private void run(int increment, boolean blocking) throws Exception {

        final List<Future<MetaReplyProto>> futures = new ArrayList<>(increment);
        final long startTime = System.currentTimeMillis();
        final ExecutorService executor = Executors.newFixedThreadPool(PEERS.size());
        PEERS.forEach(p -> {
            for (int i = 0 ; i < 5 ; i++){
                final int num = i;
                final Future<MetaReplyProto> f = CompletableFuture.supplyAsync(() -> {
                    try {
                        return ratisClient.readCover(read(String.valueOf(num)), p.getId());
                    } catch (IOException e) {
                        System.err.println("Failed read-only request");
                        return MetaReplyProto.newBuilder().setRead(ReadReplyProto.newBuilder().build()).build();
                    }
                }, executor).whenCompleteAsync((r, ex) -> {
                    final long endTime = System.currentTimeMillis();
                    final long elapsedSec = (endTime - startTime) / 1000;
                    ReadReplyProto read = r.getRead();
                    Meta meta = read.getMeta();
                    System.out.println("read from " + p.getId() + " and get key value: " + meta.getKey().toStringUtf8()
                            + ", value: " + meta.getData().toStringUtf8());
                });
                futures.add(f);
            }
        });

        for (Future<MetaReplyProto> f : futures) {
            f.get();
        }
    }

    public static void main(String[] args) {
        try (ReadDispatchClient client = new ReadDispatchClient()) {
            for (RaftPeer peer : PEERS) {
                do {
                    try {
                        GroupInfoReply info = client.client.getGroupManagementApi(peer.getId())
                                .info(RAFT_GROUP_META.getGroupId());
                        System.out.println(info);
                        break;
                    } catch (StatusRuntimeException e) {
                        Status status = e.getStatus();
                        if (status.getDescription().contains("not found")) {
                            client.client.getGroupManagementApi(peer.getId())
                                    .add(RAFT_GROUP_META);
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


    private ByteString read(String key) {
        MetaRequestProto build = MetaRequestProto.newBuilder().setRead(ReadRequestProto.newBuilder().setKey(ByteString.copyFromUtf8(key)))
                .build();
        ByteString content = build.toByteString();
        return content;
    }

    private ByteString write() {
        Random random = new Random();
        HashMap<String, String> hashMap = new HashMap<String, String>() {{
            put(String.valueOf(random.nextInt(5)), UUID.randomUUID().toString());
        }};
        return write(hashMap);
    }

    private ByteString list(String prefix) {
        return MetaRequestProto.newBuilder()
                .setList(ListRequestProto.newBuilder()
                        .setPrefix(ByteString.copyFromUtf8(prefix))
                        .build()).build().toByteString();
    }

    private ByteString write(Map<String, String> value) {
        WriteRequestProto.Builder builder = WriteRequestProto.newBuilder();
        value.forEach((k, v) -> {
            builder.addMeta(Meta.newBuilder()
                    .setKey(ByteString.copyFromUtf8(k))
                    .setData(ByteString.copyFromUtf8(v))
                    .build());
        });
        return MetaRequestProto.newBuilder().setWrite(builder).build().toByteString();
    }
}
