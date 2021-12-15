package com.jay.swarm.client;

import com.jay.swarm.client.handler.SwarmClientHandler;
import com.jay.swarm.common.config.Config;
import com.jay.swarm.common.constants.SwarmConstants;
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
import com.jay.swarm.common.network.handler.FileTransferHandler;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.common.util.FileUtil;
import com.jay.swarm.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/14 13:23
 */
@Slf4j
public class SwarmClient {
    private final BaseClient client;
    private final Serializer serializer;
    private final Config config;

    private final static String DOWNLOAD_DIR = "D:/swarm/downloads";
    public SwarmClient(Config config){
        this.config = config;
        this.client = new BaseClient();
        this.serializer = new ProtoStuffSerializer();
        FileTransferHandler transferHandler = new FileTransferHandler();
        SwarmClientHandler clientHandler = new SwarmClientHandler(transferHandler, serializer, DOWNLOAD_DIR);
        this.client.addHandler(clientHandler);
    }

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
            NetworkPacket uploadFileResponse = uploadFile(path, fileId, storages, callback);

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

    public NetworkPacket uploadFile(String path, String fileId, List<StorageInfo> storages, FileTransferCallback callback) throws Exception {
        StorageInfo storageInfo = storages.get(0);
        this.client.connect(storageInfo.getHost(), storageInfo.getPort());
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

        NetworkPacket transferEnd = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_END, fileId.getBytes(SwarmConstants.DEFAULT_CHARSET));
        // 发送END报文
        CompletableFuture<Object> future = this.client.sendAsync(transferEnd);
        // 等待结果
        NetworkPacket response =  (NetworkPacket)future.get();
        // 结束回调
        callback.onComplete(fileId, (System.currentTimeMillis() - uploadStart), file.length());
        return response;
    }


    public NetworkPacket uploadMeta(String path) throws Exception {
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

    public void download(String fileId) throws Exception {
        this.client.connect("127.0.0.1", 9999);
        NetworkPacket downloadRequest = NetworkPacket.buildPacketOfType(PacketTypes.DOWNLOAD_REQUEST, fileId.getBytes(SwarmConstants.DEFAULT_CHARSET));
        CompletableFuture<Object> future = this.client.sendAsync(downloadRequest);
        NetworkPacket response = (NetworkPacket)future.get();

    }

    public static void main(String[] args) throws Exception {
        Config config = new Config("D:/client.properties");
        SwarmClient swarmClient = new SwarmClient(config);
        swarmClient.upload("D:/01.pdf", new DefaultFileTransferCallback());
    }
}
