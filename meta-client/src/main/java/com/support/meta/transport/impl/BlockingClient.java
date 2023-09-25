package com.support.meta.transport.impl;

import com.support.meta.proto.MetaProtos.*;
import com.support.meta.transport.MetaApi;
import com.support.meta.transport.MetaTransport;
import com.support.meta.transport.MetaType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;

import static com.support.meta.proto.MetaProtos.MetaRequestProto.RequestCase.*;

public class BlockingClient extends AbstractClient implements MetaApi<ByteString, ByteString, MetaReplyProto> {

    public BlockingClient(MetaTransport metaTransport) {
        super(metaTransport.getClient());
    }

    @Override
    public MetaReplyProto metaKeys(MetaType type, ByteString prefix) {
        MetaRequestProto requestProto = doRead(LISTKEY, type.getType(), prefix);
        try {
            MetaReplyProto replyProto = getClient().readCover(requestProto.toByteString());
            return replyProto;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaReplyProto metaList(MetaType type, ByteString prefix) {
        MetaRequestProto requestProto = doRead(LIST, type.getType(), prefix);
        try {
            MetaReplyProto replyProto = getClient().readCover(requestProto.toByteString());
            return replyProto;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaReplyProto put(MetaType type, ByteString key, ByteString value) {
        MetaRequestProto requestProto = doWrite(WRITE, type.getType(), key, value);
        try {
            MetaReplyProto replyProto = getClient().writeCover(requestProto.toByteString());
            return replyProto;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaReplyProto get(MetaType type, ByteString key) {
        MetaRequestProto requestProto = doRead(READ, type.getType(), key);
        try {
            MetaReplyProto replyProto = getClient().readCover(requestProto.toByteString());
            return replyProto;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaReplyProto delete(MetaType type, ByteString key) {
        MetaRequestProto requestProto = doWrite(DELETE, type.getType(), key, null);
        try {
            MetaReplyProto replyProto = getClient().deleteCover(requestProto.toByteString());
            return replyProto;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
