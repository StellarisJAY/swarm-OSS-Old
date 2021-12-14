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

    /**
     * 存储基础目录
     */
    private final String baseDir;

    public FileTransferHandler(String baseDir) {
        this.baseDir = baseDir;
    }

    /**
     * 处理文件传输报文
     * @param packet NetworkPacket
     */
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

    /**
     * 处理传输头
     * @param packet NetworkPacket
     * @throws IOException IOException
     */
    public void handleTransferHead(NetworkPacket packet) throws IOException {
        byte[] content = packet.getContent();
        Serializer serializer = new ProtoStuffSerializer();
        // 反序列化出文件信息
        FileInfo fileInfo = serializer.deserialize(content, FileInfo.class);

        log.info("received file transfer HEAD: {}", fileInfo);
        // 为文件分配目录
        FileLocator locator = new Md5FileLocator(baseDir);
        String path = locator.locate(fileInfo.getFileId());
        // 创建appender
        FileAppender fileAppender = new FileAppender(fileInfo.getFileId(), path, fileInfo.getMd5(), fileInfo.getTotalSize(), fileInfo.getShardCount());
        appenderMap.put(fileInfo.getFileId(), fileAppender);
    }

    /**
     * 处理body
     * @param packet NetworkPacket
     * @throws IOException IOException
     */
    public void handleTransferBody(NetworkPacket packet) throws IOException {
        // 反序列化数据部分，得到文件分片
        byte[] content = packet.getContent();
        Serializer serializer = new ProtoStuffSerializer();
        FileShard shard = serializer.deserialize(content, FileShard.class);
        // 添加分片到文件拼接器
        String id = shard.getFileId();
        FileAppender appender = appenderMap.get(id);
        appender.append(shard.getContent());
    }

    /**
     * 处理传输结束
     * @param packet NetworkPacket
     */
    public void handleTransferEnd(NetworkPacket packet){
        // 解析出文件ID
        byte[] content = packet.getContent();
        String id = new String(content, StandardCharsets.UTF_8);

        log.info("received file transfer END: {}", id);
        // 拼接器complete
        FileAppender appender = appenderMap.remove(id);
        appender.complete();
    }
}
