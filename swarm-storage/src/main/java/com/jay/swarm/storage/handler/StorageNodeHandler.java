package com.jay.swarm.storage.handler;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.FileMetaStorage;
import com.jay.swarm.common.entity.FileUploadEnd;
import com.jay.swarm.common.fs.FileInfo;
import com.jay.swarm.common.fs.FileInfoCache;
import com.jay.swarm.common.fs.FileShard;
import com.jay.swarm.common.fs.locator.FileLocator;
import com.jay.swarm.common.network.BaseClient;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.network.handler.FileTransferHandler;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.storage.backup.BackupHelper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

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

    /**
     * 备份同步器
     */
    private final BackupHelper backupHelper;

    private final FileInfoCache fileInfoCache;

    public StorageNodeHandler(String storageNodeId, FileTransferHandler fileTransferHandler,
                              FileDownloadHandler downloadHandler, FileLocator locator,
                              Serializer serializer, BaseClient overseerClient,
                              FileInfoCache fileInfoCache) {
        this.fileTransferHandler = fileTransferHandler;
        this.downloadHandler = downloadHandler;
        this.locator = locator;
        this.serializer = serializer;
        this.overseerClient = overseerClient;
        this.storageNodeId = storageNodeId;
        this.fileInfoCache = fileInfoCache;
        this.backupHelper = new BackupHelper(serializer);
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
                // 处理文件数据部分
                case PacketTypes.TRANSFER_FILE_BODY: handleTransferBody(channelHandlerContext, packet);break;
                // 处理文件传输结束
                case PacketTypes.TRANSFER_FILE_END: handleTransferEnd(channelHandlerContext, packet);break;
                // 处理下载请求
                case PacketTypes.DOWNLOAD_REQUEST:
                    downloadHandler.handleDownloadRequest(channelHandlerContext, packet);
                default:break;
            }
        }catch (Exception e){
            // 处理过程异常
            log.error("channel read error: ", e);
            NetworkPacket errorResponse = NetworkPacket.buildPacketOfType(PacketTypes.ERROR,
                    e.getMessage().getBytes(SwarmConstants.DEFAULT_CHARSET));
            errorResponse.setId(packet.getId());
            channelHandlerContext.channel().writeAndFlush(errorResponse);
        }
    }

    /**
     * 处理文件头
     * @param context 上下文
     * @param packet NetworkPacket
     * @throws IOException IOException
     */
    private void handleTransferHead(ChannelHandlerContext context, NetworkPacket packet) throws IOException {
        // 反序列化文件信息
        byte[] content = packet.getContent();
        FileInfo fileInfo = serializer.deserialize(content, FileInfo.class);
        // 定位文件，即为文件分配目录
        String path = locator.locate(fileInfo.getFileId());
        // 具体处理过程
        fileTransferHandler.handleTransferHead(fileInfo, path);
        // 封装response
        NetworkPacket response = NetworkPacket.builder().id(packet.getId()).type(PacketTypes.TRANSFER_RESPONSE).build();
        context.channel().writeAndFlush(response);
    }

    /**
     * 处理文件数据
     * @param context 上下文
     * @param packet NetworkPacket
     * @throws IOException IOException
     */
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

    /**
     * 处理文件上传结束
     * @param context 上下文
     * @param packet NetworkPacket
     * @throws Exception Exception
     */
    private void handleTransferEnd(ChannelHandlerContext context, NetworkPacket packet) throws Exception{
        // 反序列化结束信息
        byte[] content = packet.getContent();
        FileUploadEnd uploadEnd = serializer.deserialize(content, FileUploadEnd.class);
        String fileId = uploadEnd.getFileId();
        // 处理END
        fileTransferHandler.handleTransferEnd(fileId);

        // 向Overseer通知存储文件结果，文件ID，节点ID
        FileMetaStorage fileMetaStorage = FileMetaStorage.builder()
                .storageId(storageNodeId).fileId(fileId).build();
        NetworkPacket noticeOverseer = NetworkPacket
                .buildPacketOfType(PacketTypes.UPDATE_FILE_META_STORAGE,
                        serializer.serialize(fileMetaStorage, FileMetaStorage.class));

        // 等待Overseer的回复
        NetworkPacket overseerResp = (NetworkPacket) overseerClient.sendAsync(noticeOverseer).get();
        NetworkPacket response = NetworkPacket.buildPacketOfType(overseerResp.getType(), overseerResp.getContent());
        response.setId(packet.getId());
        // 在备份前向用户发送回复，实现高可用性
        context.channel().writeAndFlush(response);

        /*
            查看剩余节点，进行备份接力
         */
        FileInfo fileInfo = fileInfoCache.getFileInfo(fileId);
        // 还有没有备份的节点，开始备份接力
        if(uploadEnd.getOtherStorages() != null && !uploadEnd.getOtherStorages().isEmpty()){
            backupHelper.sendBackup(fileInfo, locator.locate(fileId), uploadEnd.getOtherStorages());
        }
    }
}
