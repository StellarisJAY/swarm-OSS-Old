package com.jay.swarm.client;

import com.jay.swarm.client.handler.SwarmClientHandler;
import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.fs.FileInfo;
import com.jay.swarm.common.network.BaseClient;
import com.jay.swarm.common.network.ShardedFileSender;
import com.jay.swarm.common.network.callback.FileTransferCallback;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.network.handler.FileTransferHandler;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.common.util.FileUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

    private final static String DOWNLOAD_DIR = "D:/swarm/downloads";
    public SwarmClient(){
        this.client = new BaseClient();
        this.serializer = new ProtoStuffSerializer();
        FileTransferHandler transferHandler = new FileTransferHandler();
        SwarmClientHandler clientHandler = new SwarmClientHandler(transferHandler, serializer, DOWNLOAD_DIR);
        this.client.addHandler(clientHandler);
    }


    public void uploadFile(String path, FileTransferCallback callback) throws Exception {
        this.client.connect("127.0.0.1", 9999);
        long uploadStart = System.currentTimeMillis();
        File file = new File(path);
        byte[] md5 = FileUtil.md5(path);
        String fileId = UUID.randomUUID().toString();
        FileInfo fileInfo = FileInfo.builder().fileId(fileId)
                .md5(md5)
                .totalSize(file.length())
                .shardCount((int) (file.length() / SwarmConstants.DEFAULT_SHARD_SIZE))
                .build();
        NetworkPacket headPacket = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_HEAD, serializer.serialize(fileInfo, FileInfo.class));
        client.sendAsync(headPacket);

        // 传输文件分片
        ShardedFileSender shardedFileSender = new ShardedFileSender(client, serializer, callback);
        shardedFileSender.send(file, fileId);

        NetworkPacket transferEnd = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_END, fileId.getBytes(SwarmConstants.DEFAULT_CHARSET));

        CompletableFuture<Object> future = this.client.sendAsync(transferEnd);
        NetworkPacket response = (NetworkPacket)future.get();
        callback.onComplete(fileId, (System.currentTimeMillis() - uploadStart), file.length());
    }

    public void download(String fileId) throws ExecutionException, InterruptedException {
        this.client.connect("127.0.0.1", 9999);
        NetworkPacket downloadRequest = NetworkPacket.buildPacketOfType(PacketTypes.DOWNLOAD_REQUEST, fileId.getBytes(SwarmConstants.DEFAULT_CHARSET));
        CompletableFuture<Object> future = this.client.sendAsync(downloadRequest);
        NetworkPacket response = (NetworkPacket)future.get();
        System.out.println(response);
    }
}
