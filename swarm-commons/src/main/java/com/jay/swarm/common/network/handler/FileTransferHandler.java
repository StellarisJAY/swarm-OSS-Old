package com.jay.swarm.common.network.handler;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.fs.FileAppender;
import com.jay.swarm.common.fs.FileInfo;
import com.jay.swarm.common.fs.FileInfoCache;
import com.jay.swarm.common.fs.FileShard;
import com.jay.swarm.common.fs.locator.FileLocator;
import com.jay.swarm.common.fs.locator.Md5FileLocator;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *  文件传输处理器
 *  负责处理从从网络传输到该节点的文件
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
@Slf4j
public class FileTransferHandler {
    /**
     * 文件拼接器Map
     * key：文件ID
     */
    private final Map<String, FileAppender> appenderMap = new HashMap<>(256);

    private final FileInfoCache fileInfoCache;

    public FileTransferHandler(){
        this.fileInfoCache = null;
    }

    public FileTransferHandler(FileInfoCache fileInfoCache) {
        this.fileInfoCache = fileInfoCache;
    }

    /**
     * 处理文件传输头
     * @param fileInfo 文件信息
     * @param path 存储路径
     * @throws IOException IOException
     */
    public void handleTransferHead(FileInfo fileInfo, String path) throws IOException {
        // 创建appender
        FileAppender fileAppender = new FileAppender(fileInfo.getFileId(), path, fileInfo.getMd5(), fileInfo.getTotalSize(), fileInfo.getShardCount());
        appenderMap.put(fileInfo.getFileId(), fileAppender);
        // 缓存文件信息
        if(fileInfoCache != null){
            fileInfoCache.saveFileInfo(fileInfo);
        }
    }


    public void handleTransferBody(FileShard shard) throws IOException {
        // 添加分片到文件拼接器
        String id = shard.getFileId();
        FileAppender appender = appenderMap.get(id);
        appender.append(shard.getContent());
    }


    public void handleTransferEnd(String fileId){
        log.info("received file transfer END: {}", fileId);
        // 拼接器complete
        FileAppender appender = appenderMap.remove(fileId);
        appender.complete();
    }
}
