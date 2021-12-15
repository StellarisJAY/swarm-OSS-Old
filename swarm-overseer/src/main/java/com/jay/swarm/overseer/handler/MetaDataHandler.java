package com.jay.swarm.overseer.handler;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.*;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.common.util.StringUtils;
import com.jay.swarm.overseer.meta.MetaDataManager;
import com.jay.swarm.overseer.storage.StorageManager;
import com.jay.swarm.overseer.storage.StorageNodeSelector;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/14 19:29
 */
@Slf4j
public class MetaDataHandler {
    private final MetaDataManager metaDataManager;
    private final StorageManager storageManager;
    private final Serializer serializer;
    private final StorageNodeSelector storageNodeSelector;

    public MetaDataHandler(MetaDataManager metaDataManager, StorageManager storageManager,  Serializer serializer, StorageNodeSelector storageNodeSelector) {
        this.metaDataManager = metaDataManager;
        this.serializer = serializer;
        this.storageNodeSelector = storageNodeSelector;
        this.storageManager = storageManager;
    }

    public NetworkPacket handleUploadRequest(NetworkPacket packet){
        try{
            byte[] content = packet.getContent();
            FileUploadRequest request = serializer.deserialize(content, FileUploadRequest.class);
        /*
            参数校验
         */
            if(StringUtils.isEmpty(request.getFilename())){
                throw new IllegalArgumentException("upload request error: empty file name");
            }
            if(request.getMd5() == null || request.getMd5().length == 0){
                throw new IllegalArgumentException("upload request error: missing md5");
            }
            if(request.getSize() <= 0){
                throw new IllegalArgumentException("upload request error: invalid file size");
            }
            // 生成文件ID
            String fileId = UUID.randomUUID().toString();
            // 生成元数据
            MetaData metaData = MetaData.builder()
                    .size(request.getSize())
                    .filename(request.getFilename())
                    .key(fileId)
                    .uploadTime(System.currentTimeMillis())
                    .backupCount(request.getBackupCount())
                    .storages(new ArrayList<>())
                    .md5(request.getMd5())
                    .build();
            metaDataManager.putMetaData(metaData);
            // 选择存储节点
            List<StorageInfo> storages = storageNodeSelector.select(metaData);

            FileUploadResponse response = FileUploadResponse.builder().fileId(fileId).storageNodes(storages).build();

            NetworkPacket responsePacket = NetworkPacket.buildPacketOfType(PacketTypes.UPLOAD_RESPONSE, serializer.serialize(response, FileUploadResponse.class));
            responsePacket.setId(packet.getId());
            return responsePacket;
        }catch (Exception e){
            log.info("upload request error: {}", e.getMessage());
            if(log.isDebugEnabled()){
                log.debug("upload request error", e);
            }
            NetworkPacket errorResponse = NetworkPacket.buildPacketOfType(PacketTypes.ERROR, e.getMessage().getBytes(SwarmConstants.DEFAULT_CHARSET));
            errorResponse.setId(packet.getId());
            return errorResponse;
        }

    }

    public NetworkPacket updateFileMeta(NetworkPacket packet){
        try{
            byte[] content = packet.getContent();
            FileMetaStorage fileMetaStorage = serializer.deserialize(content, FileMetaStorage.class);
            // 获取文件meta
            MetaData metaData = metaDataManager.getMetaData(fileMetaStorage.getFileId());
            if(metaData == null){
                throw new RuntimeException("file doesn't exist");
            }
            // 寻找存储节点信息
            StorageInfo storageInfo = storageManager.getStorageInfo(fileMetaStorage.getStorageId());
            if(storageInfo == null){
                throw new RuntimeException("storage node doesn't exist");
            }
            // 更新存储节点已使用的空间
            storageInfo.setUsedStorage(storageInfo.getFreeStorage() + metaData.getSize());
            // 元数据中添加该存储节点
            metaData.getStorages().add(fileMetaStorage.getStorageId());

            NetworkPacket response = NetworkPacket.buildPacketOfType(PacketTypes.SUCCESS, "update file meta success".getBytes(SwarmConstants.DEFAULT_CHARSET));
            response.setId(packet.getId());
            return response;
        }catch (Exception e){
            e.printStackTrace();
            NetworkPacket response = NetworkPacket.buildPacketOfType(PacketTypes.ERROR, e.getMessage().getBytes(SwarmConstants.DEFAULT_CHARSET));
            response.setId(packet.getId());
            return response;
        }
    }
}
