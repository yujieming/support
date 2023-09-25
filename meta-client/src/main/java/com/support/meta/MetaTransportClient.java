package com.support.meta;

import com.support.meta.proto.MetaProtos.*;
import com.support.meta.transport.MetaApi;
import com.support.meta.transport.MetaAsyncApi;
import com.support.meta.transport.MetaTransport;
import com.support.meta.transport.impl.AsyncClient;
import com.support.meta.transport.impl.BlockingClient;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;

import java.util.function.Supplier;

public class MetaTransportClient implements MetaTransport<ByteString, ByteString, MetaReplyProto> {

    private RaftClient raftClient;

    private final Supplier<MetaApi<ByteString, ByteString, MetaReplyProto>> blockingApi;

    private final Supplier<MetaAsyncApi<ByteString, ByteString, MetaReplyProto>> asyncApi;


    public MetaTransportClient(RaftClient raftClient) {
        this.raftClient = raftClient;
        this.blockingApi = JavaUtils.memoize(() -> new BlockingClient(this));
        this.asyncApi = JavaUtils.memoize(() -> new AsyncClient(this));
    }

    @Override
    public RaftClient getClient() {
        return this.raftClient;
    }

    @Override
    public MetaApi<ByteString, ByteString, MetaReplyProto> getMetaApi() {
        return blockingApi.get();
    }

    @Override
    public MetaAsyncApi<ByteString, ByteString, MetaReplyProto> getMetaAsyncApi() {
        return asyncApi.get();
    }
}
