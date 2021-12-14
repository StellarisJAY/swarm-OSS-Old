package com.jay.swarm.storage.handler;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.fs.FileInfo;
import com.jay.swarm.common.fs.FileShard;
import com.jay.swarm.common.fs.locator.FileLocator;
import com.jay.swarm.common.fs.locator.Md5FileLocator;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.network.handler.FileTransferHandler;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * <p>
 *  StorageNode网络处理器
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
@Slf4j
@ChannelHandler.Sharable
public class StorageNodeHandler extends SimpleChannelInboundHandler<NetworkPacket> {
    /**
     * 文件传输处理器
     */
    private final FileTransferHandler fileTransferHandler;
    private final FileDownloadHandler downloadHandler;
    private final FileLocator locator;
    private final Serializer serializer;

    public StorageNodeHandler(FileTransferHandler fileTransferHandler, FileDownloadHandler downloadHandler, FileLocator locator, Serializer serializer) {
        this.fileTransferHandler = fileTransferHandler;
        this.downloadHandler = downloadHandler;
        this.locator = locator;
        this.serializer = serializer;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("channel closed by remote address: {}", ctx.channel().remoteAddress());
        ctx.channel().close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if(log.isDebugEnabled()){
            log.debug("channel handler error", cause);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, NetworkPacket packet) {
        short type = packet.getType();
        try{
            switch (type){
                // 处理文件传输请求
                case PacketTypes.TRANSFER_FILE_HEAD:
                    handleTransferHead(channelHandlerContext, packet);break;
                case PacketTypes.TRANSFER_FILE_BODY: handleTransferBody(channelHandlerContext, packet);break;
                case PacketTypes.TRANSFER_FILE_END: handleTransferEnd(channelHandlerContext, packet);break;
                case PacketTypes.DOWNLOAD_REQUEST:
                    downloadHandler.handleDownloadRequest(channelHandlerContext, packet);
                default:break;
            }
        }catch (Exception e){
            NetworkPacket errorResponse = NetworkPacket.buildPacketOfType(PacketTypes.ERROR, e.getMessage().getBytes(SwarmConstants.DEFAULT_CHARSET));
            errorResponse.setId(packet.getId());
            channelHandlerContext.channel().writeAndFlush(errorResponse);
        }
    }

    private void handleTransferHead(ChannelHandlerContext context, NetworkPacket packet) throws IOException {
        // 反序列化文件信息
        byte[] content = packet.getContent();
        FileInfo fileInfo = serializer.deserialize(content, FileInfo.class);
        String path = locator.locate(fileInfo.getFileId());
        log.info("received head pid:{}", packet.getId());
        fileTransferHandler.handleTransferHead(fileInfo, path);
        NetworkPacket response = NetworkPacket.builder().id(packet.getId()).type(PacketTypes.TRANSFER_RESPONSE).build();
        context.channel().writeAndFlush(response);
    }

    private void handleTransferBody(ChannelHandlerContext context, NetworkPacket packet) throws IOException {
        byte[] content = packet.getContent();
        FileShard shard = serializer.deserialize(content, FileShard.class);
        fileTransferHandler.handleTransferBody(shard);
        NetworkPacket response = NetworkPacket.builder().id(packet.getId()).type(PacketTypes.TRANSFER_RESPONSE).build();
        context.channel().writeAndFlush(response);
    }

    private void handleTransferEnd(ChannelHandlerContext context, NetworkPacket packet){
        byte[] content = packet.getContent();
        String fileId = new String(content, SwarmConstants.DEFAULT_CHARSET);
        fileTransferHandler.handleTransferEnd(fileId);
        NetworkPacket response = NetworkPacket.builder().id(packet.getId()).type(PacketTypes.TRANSFER_RESPONSE).build();
        context.channel().writeAndFlush(response);
    }
}
