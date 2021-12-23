package com.jay.swarm.client.download;

import com.jay.swarm.client.storage.StorageNodeSelector;
import com.jay.swarm.common.config.Config;
import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.DownloadResponse;
import com.jay.swarm.common.entity.StorageInfo;
import com.jay.swarm.common.network.BaseClient;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/16 10:52
 */
@Slf4j
public class DownloadHelper {
    private final BaseClient overseerClient;

    private final BaseClient storageClient;

    private final Serializer serializer;

    private final Config config;

    public DownloadHelper(BaseClient overseerClient, BaseClient storageClient, Serializer serializer, Config config) {
        this.overseerClient = overseerClient;
        this.storageClient = storageClient;
        this.serializer = serializer;
        this.config = config;
    }

    public DownloadResponse sendDownloadRequest(String fileId) throws Exception{
        // 获取Overseer地址
        String host = config.get("overseer.host");
        String port = config.get("overseer.port");
        if(StringUtils.isEmpty(host) || StringUtils.isEmpty(port)){
            throw new IllegalArgumentException("invalid overseer address");
        }
        // 发送下载请求
        NetworkPacket packet = NetworkPacket.buildPacketOfType(PacketTypes.DOWNLOAD_REQUEST, fileId.getBytes(SwarmConstants.DEFAULT_CHARSET));
        NetworkPacket response = (NetworkPacket)overseerClient.sendAsync(host, Integer.parseInt(port), packet).get();

        // 收到异常返回
        if(response.getType() == PacketTypes.ERROR){
            throw new RuntimeException(new String(response.getContent(), SwarmConstants.DEFAULT_CHARSET));
        }
        // 反序列化出文件元数据
        byte[] content = response.getContent();
        return serializer.deserialize(content, DownloadResponse.class);
    }

    public void pullData(DownloadResponse fileInfo) throws Exception{
        String fileId = fileInfo.getFileId();
        byte[] md5 = fileInfo.getMd5();
        List<StorageInfo> storages = fileInfo.getStorages();

        StorageInfo targetStorage = StorageNodeSelector.selectRandom(storages);

        String host = targetStorage.getHost();
        int port = targetStorage.getPort();

        NetworkPacket request = NetworkPacket.buildPacketOfType(PacketTypes.DOWNLOAD_REQUEST, fileId.getBytes(SwarmConstants.DEFAULT_CHARSET));
        NetworkPacket response = (NetworkPacket)storageClient.sendAsync(host, port, request).get();
        File file = new File("D:/swarm/downloads/" + fileId);

        if(response.getType() == PacketTypes.ERROR){
            throw new RuntimeException(new String(response.getContent(), SwarmConstants.DEFAULT_CHARSET));
        }
    }
}
