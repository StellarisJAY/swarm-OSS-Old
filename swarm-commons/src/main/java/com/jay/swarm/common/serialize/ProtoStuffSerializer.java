package com.jay.swarm.common.serialize;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 20:34
 */
public class ProtoStuffSerializer implements Serializer{

    /**
     * Schema缓存，记录每个类对应的schema，避免每次序列化生成
     */
    private static final Map<Class<?>, Schema<?>> SCHEMA_CACHE = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    @Override
    public <T> byte[] serialize(T object, Class<T> clazz){
        Schema<T> schema = getSchema(clazz);
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        try{
            return ProtostuffIOUtil.toByteArray(object, schema, buffer);
        }finally {
            buffer.clear();
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz){
        Schema<T> schema = getSchema(clazz);
        T result = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(bytes, result ,schema);
        return result;
    }

    @SuppressWarnings("unchecked")
    private <T> Schema<T> getSchema(Class<T> clazz){
        Schema<T> schema = (Schema<T>) SCHEMA_CACHE.get(clazz);
        if(schema == null){
            schema = RuntimeSchema.getSchema(clazz);
            if(schema != null) {
                SCHEMA_CACHE.put(clazz, schema);
            }
        }
        return schema;
    }
}
