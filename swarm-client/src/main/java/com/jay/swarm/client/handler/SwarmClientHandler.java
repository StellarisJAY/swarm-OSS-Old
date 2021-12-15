package com.jay.swarm.client.handler;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.fs.FileInfo;
import com.jay.swarm.common.fs.FileShard;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.network.handler.FileTransferHandler;
import com.jay.swarm.common.serialize.Serializer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

/**
 * <p>
 *  SWARM标准客户端处理器
 * </p>
 *
 * @author Jay
 * @date 2021/12/14 14:19
 */
@Slf4j
@ChannelHandler.Sharable
public class SwarmClientHandler extends SimpleChannelInboundHandler<NetworkPacket> {
    /**
     * 文件传输处理器
     */
    private final FileTransferHandler fileTransferHandler;
    private final Serializer serializer;
    private final String baseDir;

    public SwarmClientHandler(FileTransferHandler fileTransferHandler, Serializer serializer, String baseDir) {
        this.fileTransferHandler = fileTransferHandler;
        this.serializer = serializer;
        this.baseDir = baseDir;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, NetworkPacket packet) throws Exception {
        try{
            short type = packet.getType();
            switch (type){
                case PacketTypes.TRANSFER_FILE_HEAD: handleTransferHead(channelHandlerContext, packet);break;
                case PacketTypes.TRANSFER_FILE_BODY: handleTransferBody(channelHandlerContext, packet);break;
                case PacketTypes.TRANSFER_FILE_END: handleTransferEnd(channelHandlerContext, packet);break;
                default:channelHandlerContext.fireChannelRead(packet);break;
            }
        }catch (Exception e){
            if(log.isDebugEnabled()){
                log.debug("client handler error", e);
            }
        }
    }

    private void handleTransferHead(ChannelHandlerContext context, NetworkPacket packet) throws IOException {
        byte[] content = packet.getContent();
        FileInfo fileInfo = serializer.deserialize(content, FileInfo.class);
        String path = baseDir + File.separator + fileInfo.getFileId();
        fileTransferHandler.handleTransferHead(fileInfo, path);
    }

    private void handleTransferBody(ChannelHandlerContext context, NetworkPacket packet) throws IOException {
        byte[] content = packet.getContent();
        FileShard shard = serializer.deserialize(content, FileShard.class);
        log.info("received file body: {}", shard.getFileId());
        fileTransferHandler.handleTransferBody(shard);
    }

    private void handleTransferEnd(ChannelHandlerContext context, NetworkPacket packet){
        byte[] content = packet.getContent();
        String fileId = new String(content, SwarmConstants.DEFAULT_CHARSET);
        log.info("received file end: {}", fileId);
        fileTransferHandler.handleTransferEnd(fileId);
    }
}
