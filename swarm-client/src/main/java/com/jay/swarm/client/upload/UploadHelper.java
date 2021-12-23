package com.jay.swarm.client.upload;

import com.jay.swarm.client.storage.StorageNodeSelector;
import com.jay.swarm.common.config.Config;
import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.FileUploadEnd;
import com.jay.swarm.common.entity.FileUploadRequest;
import com.jay.swarm.common.entity.FileUploadResponse;
import com.jay.swarm.common.entity.StorageInfo;
import com.jay.swarm.common.fs.FileInfo;
import com.jay.swarm.common.network.BaseClient;
import com.jay.swarm.common.network.ShardedFileSender;
import com.jay.swarm.common.network.callback.DefaultFileTransferCallback;
import com.jay.swarm.common.network.callback.FileTransferCallback;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.common.util.FileUtil;
import com.jay.swarm.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.List;

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

    private final BaseClient overseerClient;
    private final BaseClient storageClient;
    private final Serializer serializer;
    private final Config config;



    public UploadHelper(BaseClient overseerClient, BaseClient storageClient, Serializer serializer, Config config) {
        this.overseerClient = overseerClient;
        this.serializer = serializer;
        this.config = config;
        this.storageClient = storageClient;
    }

    public void init() throws Exception {
        // 获取Overseer地址
        String host = config.get("overseer.host");
        String port = config.get("overseer.port");
        if(StringUtils.isEmpty(host) || StringUtils.isEmpty(port) || !port.matches("^[0-9]*$")){
            throw new IllegalArgumentException("invalid overseer address");
        }
        // 连接Overseer
        this.overseerClient.connect(host, Integer.parseInt(port));
    }




    /**
     * 上传文件
     * @param path 路径
     * @param callback 上传过程回调
     * @return 文件ID
     * @throws Exception Exception
     */
    public String upload(String path, int backupCount, FileTransferCallback callback) throws Exception {
        if(backupCount < 0 || StringUtils.isEmpty(path)){
            throw new IllegalArgumentException("wrong argument for upload ");
        }
        try{
            // 向Overseer发送上传请求，获得overseer返回的存储节点和fileId
            NetworkPacket metaResponse = uploadMeta(path, backupCount);
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
        long uploadDataStart = System.currentTimeMillis();
        /*
            选择目标存储节点
            选择一个节点作为上传点，剩下的备份由storage节点间接力传递
         */
        StorageInfo targetStorageNode = StorageNodeSelector.selectRandom(storages);

        // 计算文件MD5
        File file = new File(path);
        byte[] md5 = FileUtil.md5(path);

        // 封装文件信息，md5、ID、大小、分片个数
        FileInfo fileInfo = FileInfo.builder().fileId(fileId)
                .md5(md5).totalSize(file.length())
                .shardCount((int) (file.length() / SwarmConstants.DEFAULT_SHARD_SIZE))
                .build();
        /*
            HEAD报文
         */
        byte[] headSerialized = serializer.serialize(fileInfo, FileInfo.class);
        NetworkPacket headPacket = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_HEAD, headSerialized);
        // 发送HEAD
        storageClient.sendAsync(targetStorageNode.getHost(), targetStorageNode.getPort(), headPacket).get();

        /*
            文件分片
         */
        ShardedFileSender shardedFileSender = new ShardedFileSender(storageClient, serializer, callback);
        // 发送分片
        shardedFileSender.send(targetStorageNode.getHost(), targetStorageNode.getPort(), file, fileId);

        /*
            END 报文
         */
        FileUploadEnd uploadEnd = FileUploadEnd.builder()
                .fileId(fileId)
                // 待备份的节点
                .otherStorages(storages)
                .build();
        // 序列化、封装END报文
        byte[] endSerialized = serializer.serialize(uploadEnd, FileUploadEnd.class);
        NetworkPacket transferEnd = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_END, endSerialized);
        // 发送END报文、等待结果
        NetworkPacket response = (NetworkPacket) storageClient.sendAsync(targetStorageNode.getHost(), targetStorageNode.getPort(), transferEnd).get();
        // 结束回调
        callback.onComplete(fileId, (System.currentTimeMillis() - uploadDataStart), file.length());

        log.info("upload data finished, time used: {} ms", (System.currentTimeMillis() - uploadDataStart));

        return response;
    }


    /**
     * 上传文件元数据到Overseer
     * @param path 路径
     * @return response NetworkPacket
     * @throws Exception Exception
     */
    private NetworkPacket uploadMeta(String path, int backupCount) throws Exception {
        long uploadMetaStart = System.currentTimeMillis();
        // 获取Overseer地址
        String host = config.get("overseer.host");
        String port = config.get("overseer.port");
        // 计算文件MD5
        File file = new File(path);

        byte[] md5 = FileUtil.md5(path);

        // 创建上传请求
        FileUploadRequest request = FileUploadRequest.builder()
                .filename(file.getName())
                .size(file.length())
                .md5(md5).backupCount(backupCount + 1)
                .build();
        // 序列化、封装报文

        byte[] serializedRequest = serializer.serialize(request, FileUploadRequest.class);

        NetworkPacket requestPacket = NetworkPacket.buildPacketOfType(PacketTypes.UPLOAD_REQUEST, serializedRequest);
        // 发送上传请求，等待结果
        NetworkPacket result =  (NetworkPacket) overseerClient.sendAsync(host, Integer.parseInt(port), requestPacket).get();
        log.info("{} upload meta finished, time used {} ms", path, (System.currentTimeMillis() - uploadMetaStart));
        return result;
    }
}
