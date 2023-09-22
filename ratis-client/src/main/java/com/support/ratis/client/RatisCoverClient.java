package com.support.ratis.client;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public abstract class RatisCoverClient<V> extends RatisClient {

    public RatisCoverClient(RaftGroup group, RaftProperties properties) throws IOException {
        super(group, properties);
    }

    public RatisCoverClient(RaftGroup group, RaftProperties properties, RaftPeer primaryDataStreamServer) throws IOException {
        super(group, properties, primaryDataStreamServer);
    }

    public RatisCoverClient(RaftClient client) {
        super(client);
    }


    public abstract V cover(ByteString content);


    public V readCover(ByteString message) throws IOException {
        return cover(read(message));
    }

    public CompletableFuture<V> readAsyncCover(ByteString message) {
        return readAsync(message).thenApply(this::cover);
    }

    public V readCover(ByteString message, RaftPeerId peerId) throws IOException {
        return cover(read(message, peerId));
    }

    public CompletableFuture<V> readAsyncCover(ByteString message, RaftPeerId peerId) {
        return readAsync(message, peerId).thenApply(this::cover);
    }

    public V writeCover(ByteString message) throws IOException {
        return cover(write(message));
    }

    public CompletableFuture<V> writeAsyncCover(ByteString message) {
        return writeAsync(message).thenApply(this::cover);
    }

    public V deleteCover(ByteString message) throws IOException {
        return cover(delete(message));
    }

    public CompletableFuture<V> deleteAsyncCover(ByteString message) {
        return deleteAsync(message).thenApply(this::cover);
    }
}
