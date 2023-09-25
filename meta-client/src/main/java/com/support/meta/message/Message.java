package com.support.meta.message;

import com.support.meta.transport.MetaType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.*;

public interface Message extends Serializable {

    MetaType getType();

    default ByteString toByteString() {
        return writeObject2ByteString(this);
    }

    static ByteString writeObject2ByteString(Object obj) {
        final ByteString.Output byteOut = ByteString.newOutput();
        try (ObjectOutputStream objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(obj);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Unexpected IOException when writing an object to a ByteString.", e);
        }
        return byteOut.toByteString();
    }

    static <T extends Message> T toMessage(ByteString bytes, Class<T> clazz) {

        return readObject(bytes.newInput(), clazz);
    }

    static byte[] object2Bytes(Object obj) {
        return writeObject2ByteString(obj).toByteArray();
    }

    static <T extends Message> T bytes2Message(byte[] bytes, Class<T> clazz) {
        return readObject(new ByteArrayInputStream(bytes), clazz);
    }

    static <T extends Message> T readObject(InputStream in, Class<T> clazz) {
        final Object obj;
        try (ObjectInputStream oin = new ObjectInputStream(in)) {
            obj = oin.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("Failed to readObject for class " + clazz, e);
        }
        try {
            return clazz.cast(obj);
        } catch (ClassCastException e) {
            throw new IllegalStateException("Failed to cast to " + clazz + ", object="
                    + obj, e);
        }
    }
}
