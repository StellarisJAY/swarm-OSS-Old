package com.jay.swarm.overseer.handler;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.DownloadResponse;
import com.jay.swarm.common.entity.MetaData;
import com.jay.swarm.common.entity.StorageInfo;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.overseer.meta.MetaDataManager;
import com.jay.swarm.overseer.storage.StorageManager;
import com.jay.swarm.overseer.storage.StorageNodeSelector;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 16:17
 */
@ChannelHandler.Sharable
@Slf4j
public class PacketHandler extends SimpleChannelInboundHandler<NetworkPacket> {

    private final StorageHandler storageHandler;
    private final MetaDataHandler metaDataHandler;
    private final MetaDataManager metaDataManager;
    private final StorageManager storageManager;
    private final Serializer serializer;

    public PacketHandler(StorageManager storageManager, MetaDataManager metaDataManager, Serializer serializer) {
        this.metaDataManager = metaDataManager;
        this.storageManager = storageManager;
        this.storageHandler = new StorageHandler(storageManager, serializer);
        StorageNodeSelector storageNodeSelector = new StorageNodeSelector(storageManager);
        this.metaDataHandler = new MetaDataHandler(metaDataManager, storageManager, serializer, storageNodeSelector);
        this.serializer = serializer;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("connection closed by remote address {}", ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)  {
        if(log.isDebugEnabled()){
            log.debug("exception caught from channel: " + ctx.channel().remoteAddress(), cause);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NetworkPacket packet) {
        short packetType = packet.getType();
        NetworkPacket response = null;
        // 根据报文类型做不同处理，最终得到一个Response packet
        switch(packetType){
            // 注册存储节点
            case PacketTypes.STORAGE_REGISTER: response = storageHandler.handleStorageRegister(packet, ctx.channel());break;
            // 存储节点心跳
            case PacketTypes.HEART_BEAT: response = storageHandler.handleStorageHeartBeat(packet); break;
            // 上传请求
            case PacketTypes.UPLOAD_REQUEST: response = metaDataHandler.handleUploadRequest(packet); break;
            // 更新meta
            case PacketTypes.UPDATE_FILE_META_STORAGE: response = metaDataHandler.updateFileMeta(packet); break;

            case PacketTypes.DOWNLOAD_REQUEST: response = handleDownloadRequest(packet);break;
            default:break;
        }
        if(response != null){
            // 发送response
            ctx.channel().writeAndFlush(response);
        }
    }

    private NetworkPacket handleDownloadRequest(NetworkPacket packet){
        try{
            byte[] content = packet.getContent();
            String fileId = new String(content, SwarmConstants.DEFAULT_CHARSET);
            // 获取文件元数据
            MetaData metaData = metaDataManager.getMetaData(fileId);
            // 文件不存在
            if(metaData == null){
                throw new RuntimeException("file " + fileId + " doesn't exist");
            }
            // 文件存储的节点集合
            List<String> storages = metaData.getStorages();

            if(storages == null || storages.isEmpty()){
                throw new RuntimeException("no storage node contains file " + fileId);
            }

            // 筛选出存活节点的信息
            List<StorageInfo> aliveNodes = storageManager.getAliveNodes(storages);

            if(aliveNodes == null || aliveNodes.isEmpty()){
                throw new RuntimeException("no alive storage node contains file " + fileId);
            }

            DownloadResponse response = DownloadResponse.builder()
                    .md5(metaData.getMd5())
                    .size(metaData.getSize())
                    .fileId(fileId)
                    .storages(aliveNodes)
                    .build();

            NetworkPacket respPacket = NetworkPacket.buildPacketOfType(PacketTypes.SUCCESS, serializer.serialize(response, DownloadResponse.class));
            respPacket.setId(packet.getId());
            return respPacket;
        }catch (Exception e){
            NetworkPacket errorResponse = NetworkPacket.buildPacketOfType(PacketTypes.ERROR, e.getMessage().getBytes(SwarmConstants.DEFAULT_CHARSET));
            errorResponse.setId(packet.getId());
            return errorResponse;
        }
    }
}
