package com.jay.swarm.client.upload;

import com.jay.swarm.common.config.Config;
import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.FileUploadEnd;
import com.jay.swarm.common.entity.FileUploadRequest;
import com.jay.swarm.common.entity.FileUploadResponse;
import com.jay.swarm.common.entity.StorageInfo;
import com.jay.swarm.common.fs.FileInfo;
import com.jay.swarm.common.network.BaseClient;
import com.jay.swarm.common.network.ShardedFileSender;
import com.jay.swarm.common.network.callback.FileTransferCallback;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.common.util.FileUtil;
import com.jay.swarm.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * <p>
 *  上传文件
 * </p>
 *
 * @author Jay
 * @date 2021/12/15 16:18
 */
@Slf4j
public final class UploadHelper {

    private final BaseClient client;
    private final Serializer serializer;
    private final Config config;

    public UploadHelper(BaseClient client, Serializer serializer, Config config) {
        this.client = client;
        this.serializer = serializer;
        this.config = config;
    }

    /**
     * 上传文件
     * @param path 路径
     * @param callback 上传过程回调
     * @return 文件ID
     * @throws Exception Exception
     */
    public String upload(String path, FileTransferCallback callback) throws Exception {
        try{
            // 向Overseer发送上传请求，获得overseer返回的存储节点和fileId
            NetworkPacket metaResponse = uploadMeta(path);
            short metaResponseType = metaResponse.getType();
            // Overseer返回错误
            if(metaResponseType == PacketTypes.ERROR){
                String message = new String(metaResponse.getContent(), SwarmConstants.DEFAULT_CHARSET);
                throw new IllegalStateException(message);
            }
            // 解析Overseer的返回
            byte[] content = metaResponse.getContent();
            FileUploadResponse fileUploadResponse = serializer.deserialize(content, FileUploadResponse.class);
            String fileId = fileUploadResponse.getFileId();
            List<StorageInfo> storages = fileUploadResponse.getStorageNodes();

            // 向storage发送文件
            NetworkPacket uploadFileResponse = uploadFileData(path, fileId, storages, callback);

            // storageNode返回错误
            if(uploadFileResponse.getType() == PacketTypes.ERROR){
                String message = new String(uploadFileResponse.getContent(), SwarmConstants.DEFAULT_CHARSET);
                throw new IllegalStateException(message);
            }
            // 最后返回文件ID
            return fileId;
        }catch (Exception e){
            if(log.isDebugEnabled()){
                log.debug("upload error: ", e);
            }
            throw e;
        }
    }

    /**
     * 发送文件数据到StorageNode
     * @param path 路径
     * @param fileId 文件ID
     * @param storages 目标存储节点
     * @param callback 回调
     * @return response NetworkPacket
     * @throws Exception Exception
     */
    private NetworkPacket uploadFileData(String path, String fileId, List<StorageInfo> storages, FileTransferCallback callback) throws Exception {
        // 选择目标存储节点
        StorageInfo targetStorageNode = StorageNodeSelector.select(storages);

        // 连接目标节点
        this.client.connect(targetStorageNode.getHost(), targetStorageNode.getPort());
        // 上传文件开始时间
        long uploadStart = System.currentTimeMillis();
        File file = new File(path);
        // 计算文件MD5
        byte[] md5 = FileUtil.md5(path);
        // 封装文件信息，md5、ID、大小、分片个数
        FileInfo fileInfo = FileInfo.builder().fileId(fileId)
                .md5(md5)
                .totalSize(file.length())
                .shardCount((int) (file.length() / SwarmConstants.DEFAULT_SHARD_SIZE))
                .build();
        // 发送HEAD报文
        NetworkPacket headPacket = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_HEAD, serializer.serialize(fileInfo, FileInfo.class));
        client.sendAsync(headPacket);

        // 传输文件分片
        ShardedFileSender shardedFileSender = new ShardedFileSender(client, serializer, callback);
        shardedFileSender.send(file, fileId);

        // END 报文，包括剩下的节点
        FileUploadEnd uploadEnd = FileUploadEnd.builder().fileId(fileId).otherStorages(storages).build();
        NetworkPacket transferEnd = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_END,
                serializer.serialize(uploadEnd, FileUploadEnd.class));
        // 发送END报文
        CompletableFuture<Object> future = this.client.sendAsync(transferEnd);
        // 等待结果
        NetworkPacket response =  (NetworkPacket)future.get();
        // 结束回调
        callback.onComplete(fileId, (System.currentTimeMillis() - uploadStart), file.length());
        return response;
    }


    /**
     * 上传文件元数据到Overseer
     * @param path 路径
     * @return response NetworkPacket
     * @throws Exception Exception
     */
    private NetworkPacket uploadMeta(String path) throws Exception {
        String host = config.get("overseer.host");
        String port = config.get("overseer.port");
        if(StringUtils.isEmpty(host) || StringUtils.isEmpty(port) || !port.matches("^[0-9]*$")){
            throw new IllegalArgumentException("invalid overseer address");
        }
        // 连接Overseer
        this.client.connect(host, Integer.parseInt(port));
        // 计算文件MD5
        File file = new File(path);
        byte[] md5 = FileUtil.md5(path);
        // 创建上传请求
        FileUploadRequest request = FileUploadRequest.builder().filename(file.getName()).size(file.length()).md5(md5).backupCount(1).build();
        // 封装报文
        NetworkPacket packet = NetworkPacket.buildPacketOfType(PacketTypes.UPLOAD_REQUEST, serializer.serialize(request, FileUploadRequest.class));
        // 发送上传请求，并等待结果
        CompletableFuture<Object> future = this.client.sendAsync(packet);
        return (NetworkPacket) future.get();
    }
}
