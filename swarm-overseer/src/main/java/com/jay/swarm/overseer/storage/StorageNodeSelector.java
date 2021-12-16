package com.jay.swarm.overseer.storage;

import com.jay.swarm.common.entity.MetaData;
import com.jay.swarm.common.entity.StorageInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  存储节点选择器
 *  从存储节点中一定数量的存活的、空间足够的节点
 * </p>
 *
 * @author Jay
 * @date 2021/12/14 20:08
 */
@Slf4j
public class StorageNodeSelector {
    private final StorageManager storageManager;

    public StorageNodeSelector(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    public List<StorageInfo> select(MetaData metaData){
        int backupCount = metaData.getBackupCount();
        // 获取存活存储节点
        List<StorageInfo> aliveNodes = storageManager.listAliveNodes();
        // 存储节点数量不足
        if(backupCount > aliveNodes.size()){
            throw new RuntimeException("no enough storage nodes for upload and backup");
        }
        // 计算存储节点预期的平均用量百分比
        double averagePercentage = 0.0;
        for (StorageInfo node : aliveNodes) {
            averagePercentage += (double)(metaData.getSize() + node.getUsedStorage()) / (node.getUsedStorage() + node.getFreeStorage());
        }
        averagePercentage /= aliveNodes.size();
        double average = averagePercentage;

        // 第一次筛选，筛选预期容量百分比小于平均容量百分比的节点
        List<StorageInfo> firstSelections = aliveNodes.stream().filter(storage -> {
            double percentage = (double) (metaData.getSize() + storage.getUsedStorage()) / (storage.getUsedStorage() + storage.getFreeStorage());
            return percentage < average;
        }).collect(Collectors.toList());

        List<StorageInfo> finalSelections = new ArrayList<>(backupCount);
        finalSelections.addAll(firstSelections);
        // 第二次筛选，筛选出第一次外的容量足够的节点
        if(firstSelections.size() < backupCount){
            for(StorageInfo storageInfo : aliveNodes){
                if(backupCount == 0) {
                    break;
                }
                else if(!firstSelections.contains(storageInfo) ){
                    finalSelections.add(storageInfo);
                    backupCount--;
                }
            }
        }
        else{
            finalSelections = firstSelections;
        }

        if(finalSelections.size() < backupCount){
            throw new RuntimeException("no enough storage nodes for upload and backup");
        }

        return finalSelections;
    }
}
