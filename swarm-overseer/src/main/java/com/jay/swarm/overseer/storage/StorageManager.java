package com.jay.swarm.overseer.storage;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.Storage;
import com.jay.swarm.overseer.config.OverseerConfig;

import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *  存储节点管理器
 * </p>
 *
 * @author Jay
 * @date 2021/12/10
 **/
public class StorageManager {
    private final OverseerConfig config;
    private final ConcurrentHashMap<String, Storage> storages = new ConcurrentHashMap<>(256);

    public StorageManager(OverseerConfig config) {
        this.config = config;
    }

    /**
     * 注册存储节点
     * 该方法在收到某个节点发送的注册报文后调用
     * @param storage storage对象，由网络报文解析出的存储节点信息
     * @return boolean 注册是否成功
     */
    public synchronized boolean registerStorage(Storage storage){
        String storageId = storage.getId();
        // 设置新节点的心跳时间
        storage.setLastHeartBeatTime(System.currentTimeMillis());
        // 查看已经用该ID注册的存储节点
        Storage existingStorage = storages.get(storageId);
        if(existingStorage == null){
            // 不存在该ID节点
            storages.put(storageId, storage);
        }
        else{
            // 检查已注册节点的上次心跳
            long lastHeartBeatTime = existingStorage.getLastHeartBeatTime();
            if((System.currentTimeMillis() - lastHeartBeatTime) > SwarmConstants.DEFAULT_HEARTBEAT_PERIOD){
                // 上次心跳超出了心跳间隔，判断该节点已经死亡，新节点作为替换
                storages.put(storageId, storage);
            }
            else if(existingStorage.getHost().equals(storage.getHost()) && existingStorage.getPort() == storage.getPort()){
                // 没有死亡，但是是相同地址节点的注册请求，将该请求视为心跳，更新信息
                storages.put(storageId, storage);
            }
            else{
                // ID已经占用，占用者存活且地址信息不同，注册失败
                return false;
            }
        }
        return true;
    }

    public void storageHeartBeat(Storage storage) throws IllegalAccessException {
        Storage existingStorage = storages.get(storage.getId());
        // 节点不存在 或者 节点地址不匹配
        if(existingStorage == null || !storage.getHost().equals(existingStorage.getHost()) || storage.getPort() != existingStorage.getPort()){
            throw new IllegalAccessException("heat beat failed, storage node info mismatch");
        }
        // 更新心跳时间
        existingStorage.setLastHeartBeatTime(System.currentTimeMillis());
        existingStorage.setDiskUsagePercent(storage.getDiskUsagePercent());
    }
}
