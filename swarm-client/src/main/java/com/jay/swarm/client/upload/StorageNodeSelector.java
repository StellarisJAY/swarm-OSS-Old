package com.jay.swarm.client.upload;

import com.jay.swarm.common.entity.StorageInfo;

import java.util.Comparator;
import java.util.List;

/**
 * <p>
 *  客户端只从目标节点集合选择一个作为上传点
 *  其他节点通过“接力”方式传递副本
 * </p>
 *
 * @author Jay
 * @date 2021/12/15 16:19
 */
public class StorageNodeSelector {
    /**
     * 选择节点，优先选择最近心跳的节点
     * @param storages 存储节点集合
     * @return 最终上传点
     */
    public static StorageInfo select(List<StorageInfo> storages){
        if(storages.size() == 1){
            return storages.remove(0);
        }
        // 根据上次心跳时间排序，降序
        storages.sort(new Comparator<StorageInfo>() {
            @Override
            public int compare(StorageInfo o1, StorageInfo o2) {
                return (int)(o2.getLastHeartBeatTime() - o1.getLastHeartBeatTime());
            }
        });
        // 返回第一个
        return storages.remove(0);
    }
}
