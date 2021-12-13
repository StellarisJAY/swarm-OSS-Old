package com.jay.swarm.common.network.handler;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.fs.FileAppender;
import com.jay.swarm.common.fs.FileInfo;
import com.jay.swarm.common.fs.FileShard;
import com.jay.swarm.common.fs.locator.FileLocator;
import com.jay.swarm.common.fs.locator.Md5FileLocator;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
@Slf4j
public class FileTransferHandler {
    private final Map<String, FileAppender> appenderMap = new HashMap<>(256);

    private final String baseDir;

    public FileTransferHandler(String baseDir) {
        this.baseDir = baseDir;
    }

    public void handle(NetworkPacket packet){
        try{
            switch(packet.getType()){
                case PacketTypes.TRANSFER_FILE_HEAD: handleTransferHead(packet);break;
                case PacketTypes.TRANSFER_FILE_BODY: handleTransferBody(packet);break;
                case PacketTypes.TRANSFER_FILE_END: handleTransferEnd(packet);break;
                default:break;
            }
        }catch (Exception e){
            log.error("file transfer error: " ,e);
        }finally {
            if(packet != null && packet.getType() == PacketTypes.TRANSFER_FILE_END){
                String id = new String(packet.getContent(), SwarmConstants.DEFAULT_CHARSET);
                FileAppender appender = appenderMap.remove(id);
                appender.release();
            }
        }
    }


    public void handleTransferHead(NetworkPacket packet) throws IOException {
        byte[] content = packet.getContent();
        Serializer serializer = new ProtoStuffSerializer();
        // 反序列化出文件信息
        FileInfo fileInfo = serializer.deserialize(content, FileInfo.class);

        log.info("received file transfer HEAD: {}", fileInfo);

        FileLocator locator = new Md5FileLocator(baseDir);
        String path = locator.locate(fileInfo.getFileId());
        // 创建appender
        FileAppender fileAppender = new FileAppender(fileInfo.getFileId(), path, fileInfo.getMd5(), fileInfo.getTotalSize(), fileInfo.getShardCount());
        appenderMap.put(fileInfo.getFileId(), fileAppender);
    }

    public void handleTransferBody(NetworkPacket packet) throws IOException {
        byte[] content = packet.getContent();
        Serializer serializer = new ProtoStuffSerializer();
        FileShard shard = serializer.deserialize(content, FileShard.class);
        String id = shard.getFileId();
        FileAppender appender = appenderMap.get(id);
        appender.append(shard.getContent());
    }

    public void handleTransferEnd(NetworkPacket packet){
        byte[] content = packet.getContent();
        String id = new String(content, StandardCharsets.UTF_8);

        log.info("received file transfer END: {}", id);

        FileAppender appender = appenderMap.get(id);
        appender.complete();
    }
}
