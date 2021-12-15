package com.jay.swarm.storage.handler;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.FileMetaStorage;
import com.jay.swarm.common.fs.FileInfo;
import com.jay.swarm.common.fs.FileShard;
import com.jay.swarm.common.fs.locator.FileLocator;
import com.jay.swarm.common.fs.locator.Md5FileLocator;
import com.jay.swarm.common.network.BaseClient;
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
    private final BaseClient overseerClient;
    private final String storageNodeId;

    public StorageNodeHandler(String storageNodeId, FileTransferHandler fileTransferHandler, FileDownloadHandler downloadHandler, FileLocator locator, Serializer serializer, BaseClient overseerClient) {
        this.fileTransferHandler = fileTransferHandler;
        this.downloadHandler = downloadHandler;
        this.locator = locator;
        this.serializer = serializer;
        this.overseerClient = overseerClient;
        this.storageNodeId = storageNodeId;
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
            log.error("channel read error: ", e);
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
        fileTransferHandler.handleTransferHead(fileInfo, path);
        NetworkPacket response = NetworkPacket.builder().id(packet.getId()).type(PacketTypes.TRANSFER_RESPONSE).build();
        context.channel().writeAndFlush(response);
    }

    private void handleTransferBody(ChannelHandlerContext context, NetworkPacket packet) throws IOException {
        // 反序列化分片
        byte[] content = packet.getContent();
        FileShard shard = serializer.deserialize(content, FileShard.class);
        // 处理分片
        fileTransferHandler.handleTransferBody(shard);
        // 回复报文
        NetworkPacket response = NetworkPacket.builder().id(packet.getId()).type(PacketTypes.TRANSFER_RESPONSE).build();
        context.channel().writeAndFlush(response);
    }

    private void handleTransferEnd(ChannelHandlerContext context, NetworkPacket packet) throws Exception{
        byte[] content = packet.getContent();
        String fileId = new String(content, SwarmConstants.DEFAULT_CHARSET);
        fileTransferHandler.handleTransferEnd(fileId);

        // 向Overseer通知存储文件结果，文件ID，节点ID
        FileMetaStorage fileMetaStorage = FileMetaStorage.builder()
                .storageId(storageNodeId).fileId(fileId).build();
        NetworkPacket noticeOverseer = NetworkPacket.buildPacketOfType(PacketTypes.UPDATE_FILE_META_STORAGE, serializer.serialize(fileMetaStorage, FileMetaStorage.class));
        // 等待Overseer的回复
        NetworkPacket overseerResp = (NetworkPacket) overseerClient.sendAsync(noticeOverseer).get();
        NetworkPacket response = NetworkPacket.buildPacketOfType(overseerResp.getType(), overseerResp.getContent());
        response.setId(packet.getId());
        // 向客户端的回复
        context.channel().writeAndFlush(response);
    }
}
