package com.jay.swarm.overseer.meta;

import com.jay.swarm.common.entity.MetaData;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.common.util.AppendableByteArray;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *  meta-data管理器
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 20:15
 */
public class MetaDataManager {
    /**
     * 元数据缓存
     * key：文件ID，value：文件元数据。
     * 元数据可以被持久化到磁盘文件，目前准备使用序列化的方式将该Map序列化写入持久化文件.
     * 同样使用序列化后的Map向Guardian同步元数据
     */
    private final ConcurrentHashMap<String, MetaData> metaCache = new ConcurrentHashMap<>(256);


    /**
     * 获取元数据缓存的快照副本
     * @return List
     */
    protected List<MetaData> copyOfCache(){
        // 对元数据缓存加锁，然后复制整个缓存
        synchronized (metaCache){
            Collection<MetaData> values = metaCache.values();
            return new ArrayList<>(values);
        }
    }

    public void putMetaData(MetaData metaData){
        metaCache.put(metaData.getKey(), metaData);
    }

    public MetaData getMetaData(String fileId){
        return metaCache.get(fileId);
    }

}
