package com.hc.formats.protobuf.row;

import com.google.protobuf.Message;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class ProtobufSerializationSchema<T extends Message> implements SerializationSchema<T> {

    @Override
    public byte[] serialize(T t) {
        return t.toByteArray();
    }

}
