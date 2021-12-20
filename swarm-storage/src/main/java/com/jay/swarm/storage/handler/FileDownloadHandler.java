package com.jay.swarm.storage.handler;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.fs.FileInfo;
import com.jay.swarm.common.fs.FileInfoCache;
import com.jay.swarm.common.network.ShardedFileSender;
import com.jay.swarm.common.network.callback.DefaultFileTransferCallback;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.Serializer;
import io.netty.channel.ChannelHandlerContext;

import java.io.File;

/**
 * <p>
 *  下载处理器
 * </p>
 *
 * @author Jay
 * @date 2021/12/14 10:43
 */
public class FileDownloadHandler {
    /**
     * 文件信息缓存
     * 缓存md5、文件路径等信息
     */
    private final FileInfoCache fileInfoCache;
    private final Serializer serializer;

    public FileDownloadHandler(FileInfoCache cache, Serializer serializer){
        this.fileInfoCache = cache;
        this.serializer =serializer;
    }

    public void handleDownloadRequest(ChannelHandlerContext ctx, NetworkPacket packet){
        try{
            // 解析出文件ID
            byte[] content = packet.getContent();
            String fileId = new String(content, SwarmConstants.DEFAULT_CHARSET);
            // 获取文件路径
            String path = fileInfoCache.getPath(fileId);
            FileInfo fileInfo = fileInfoCache.getFileInfo(fileId);
            if(fileInfo == null){
                throw new RuntimeException("file doesn't exist");
            }
            // 序列化&封装HEAD，发送HEAD
            byte[] headContent = serializer.serialize(fileInfo, FileInfo.class);
            NetworkPacket headPacket = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_HEAD, headContent);
            ctx.channel().writeAndFlush(headPacket);

            // 发送文件BODY
            File file = new File(path);
            ShardedFileSender shardedFileSender = new ShardedFileSender(ctx.channel(), serializer, new DefaultFileTransferCallback());
            shardedFileSender.send(file, fileId);
            // 发送END
            NetworkPacket endPacket = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_END, fileId.getBytes(SwarmConstants.DEFAULT_CHARSET));
            endPacket.setId(packet.getId());
            ctx.channel().writeAndFlush(endPacket);
        }catch (Exception e){
            e.printStackTrace();
            NetworkPacket errorPacket = NetworkPacket.builder().type(PacketTypes.ERROR).id(packet.getId()).content(e.getMessage().getBytes(SwarmConstants.DEFAULT_CHARSET)).build();
            ctx.channel().writeAndFlush(errorPacket);
        }

    }
}
