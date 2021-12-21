package com.jay.swarm.overseer.meta;

import com.jay.swarm.common.entity.Bucket;
import com.jay.swarm.common.entity.MetaData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
     * 存储桶缓存
     * key：桶ID，value：桶信息
     */
    private final ConcurrentHashMap<String, Bucket> bucketCache = new ConcurrentHashMap<>(256);
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
        synchronized (this){
            Collection<MetaData> values = metaCache.values();
            return new ArrayList<>(values);
        }
    }

    /**
     * 获取目标ID的元数据快照
     * @param keys 文件IDs
     * @return List
     */
    protected List<MetaData> copyOfCache(List<String> keys){
        synchronized (metaCache){
            return metaCache.values().stream().filter(metaData -> {
                return keys.contains(metaData.getKey());
            }).collect(Collectors.toList());
        }
    }

    protected List<Bucket> listBuckets(){
        return new ArrayList<>(bucketCache.values());
    }

    public void putMetaData(MetaData metaData){
        metaCache.put(metaData.getKey(), metaData);
    }

    /**
     * 获取元数据
     * @param fileId 文件ID
     * @return MetaData
     */
    public MetaData getMetaData(String fileId){
        return metaCache.get(fileId);
    }

    /**
     * 保存桶
     * @param bucket 桶
     */
    public void saveBucket(Bucket bucket){
        bucketCache.put(bucket.getId(), bucket);
    }
}
