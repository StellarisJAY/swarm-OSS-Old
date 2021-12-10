package com.jay.swarm.common.serialize;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 20:32
 */
public interface Serializer {

    <T> byte[] serialize(T object, Class<T> clazz);

    <T> T deserialize(byte[] bytes, Class<T> clazz);
}
