package com.jay.swarm.overseer.storage;

import com.jay.swarm.common.config.Config;
import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.StorageInfo;
import io.netty.channel.Channel;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * <p>
 *  存储节点管理器
 *  相当于存储节点的注册中心，记录了存储节点的信息和channel
 * </p>
 *
 * @author Jay
 * @date 2021/12/10
 **/
public class StorageManager {
    private final Config config;
    /**
     * storage节点信息
     */
    private final ConcurrentHashMap<String, StorageInfo> storages = new ConcurrentHashMap<>(256);

    /**
     * storage节点连接
     */
    private final ConcurrentHashMap<String, Channel> storageChannels = new ConcurrentHashMap<>(256);

    public StorageManager(Config config) {
        this.config = config;
    }

    public List<StorageInfo> listAliveNodes(){
        return storages.values().stream()
                .filter(storage -> {
                    return System.currentTimeMillis() - storage.getLastHeartBeatTime() < SwarmConstants.DEFAULT_HEARTBEAT_PERIOD;
                }).collect(Collectors.toList());
    }

    public StorageInfo getStorageInfo(String storageId){
        return storages.get(storageId);
    }

    /**
     * 注册存储节点，且保存该节点的channel
     * 该方法在收到某个节点发送的注册报文后调用
     * @param storageInfo storage对象，由网络报文解析出的存储节点信息
     * @return boolean 注册是否成功
     */
    public synchronized boolean registerStorage(StorageInfo storageInfo, Channel channel){
        String storageId = storageInfo.getId();
        // 设置新节点的心跳时间
        storageInfo.setLastHeartBeatTime(System.currentTimeMillis());
        // 查看已经用该ID注册的存储节点
        StorageInfo existingStorage = storages.get(storageId);
        if(existingStorage == null){
            // 不存在该ID节点
            storages.put(storageId, storageInfo);
            storageChannels.put(storageId, channel);
        }
        else{
            // 检查已注册节点的上次心跳
            long lastHeartBeatTime = existingStorage.getLastHeartBeatTime();
            if((System.currentTimeMillis() - lastHeartBeatTime) > SwarmConstants.DEFAULT_HEARTBEAT_PERIOD){
                // 上次心跳超出了心跳间隔，判断该节点已经死亡，新节点作为替换
                storages.put(storageId, storageInfo);
                storageChannels.put(storageId, channel);
            }
            else if(existingStorage.getHost().equals(storageInfo.getHost()) && existingStorage.getPort() == storageInfo.getPort()){
                // 没有死亡，但是是相同地址节点的注册请求，将该请求视为心跳，更新信息
                storages.put(storageId, storageInfo);
                storageChannels.put(storageId, channel);
            }
            else{
                // ID已经占用，占用者存活且地址信息不同，注册失败
                return false;
            }
        }
        return true;
    }

    public void storageHeartBeat(StorageInfo storageInfo) throws IllegalAccessException {
        StorageInfo existingStorage = storages.get(storageInfo.getId());
        // 节点不存在 或者 节点地址不匹配
        if(existingStorage == null || !storageInfo.getHost().equals(existingStorage.getHost()) || storageInfo.getPort() != existingStorage.getPort()){
            throw new IllegalAccessException("heat beat failed, storage node info mismatch");
        }
        // 更新心跳时间
        existingStorage.setLastHeartBeatTime(System.currentTimeMillis());
        existingStorage.setFreeStorage(storageInfo.getFreeStorage());
        existingStorage.setUsedStorage(storageInfo.getUsedStorage());
    }

    /**
     * 删除channel，channelInactive后删除，避免浪费Map的空间
     * @param channel channel
     * @return 是否删除
     */
    public boolean removeChannel(Channel channel){
        Set<Map.Entry<String, Channel>> entries = storageChannels.entrySet();
        String key = null;
        for (Map.Entry<String, Channel> entry : entries) {
            if(entry.getValue() == channel){
                key = entry.getKey();
                break;
            }
        }
        if(key == null){
            return false;
        }
        storageChannels.remove(key);
        return true;
    }
}
