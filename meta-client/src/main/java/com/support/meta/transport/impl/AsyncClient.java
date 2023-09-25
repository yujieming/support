package com.support.meta.transport.impl;

import com.support.meta.proto.MetaProtos.*;
import com.support.meta.transport.MetaAsyncApi;
import com.support.meta.transport.MetaTransport;
import com.support.meta.transport.MetaType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.concurrent.CompletableFuture;

import static com.support.meta.proto.MetaProtos.MetaRequestProto.RequestCase.*;

public class AsyncClient extends AbstractClient implements MetaAsyncApi<ByteString, ByteString, MetaReplyProto> {

    public AsyncClient(MetaTransport metaTransport) {
        super(metaTransport.getClient());
    }

    @Override
    public CompletableFuture<MetaReplyProto> metaKeys(MetaType type, ByteString prefix) {
        MetaRequestProto requestProto = doRead(LISTKEY, type.getType(), prefix);
        return getClient().readAsyncCover(requestProto.toByteString());
    }

    @Override
    public CompletableFuture<MetaReplyProto> metaList(MetaType type, ByteString prefix) {
        MetaRequestProto requestProto = doRead(LIST, type.getType(), prefix);
        return getClient().readAsyncCover(requestProto.toByteString());
    }

    @Override
    public CompletableFuture<MetaReplyProto> put(MetaType type, ByteString key, ByteString value) {
        MetaRequestProto requestProto = doWrite(WRITE, type.getType(), key, value);
        return getClient().writeAsyncCover(requestProto.toByteString());
    }

    @Override
    public CompletableFuture<MetaReplyProto> get(MetaType type, ByteString key) {
        MetaRequestProto requestProto = doRead(READ, type.getType(), key);
        return getClient().readAsyncCover(requestProto.toByteString());
    }

    @Override
    public CompletableFuture<MetaReplyProto> delete(MetaType type, ByteString key) {
        MetaRequestProto requestProto = doWrite(DELETE, type.getType(), key, null);
        return getClient().deleteAsyncCover(requestProto.toByteString());
    }
}
