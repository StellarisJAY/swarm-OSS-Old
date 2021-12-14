package com.jay.swarm.storage.handler;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.fs.FileInfo;
import com.jay.swarm.common.fs.FileInfoCache;
import com.jay.swarm.common.fs.FileShard;
import com.jay.swarm.common.network.ShardedFileSender;
import com.jay.swarm.common.network.callback.DefaultFileTransferCallback;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import io.netty.channel.ChannelHandlerContext;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/14 10:43
 */
public class FileDownloadHandler {
    private final FileInfoCache fileInfoCache;
    public FileDownloadHandler(FileInfoCache cache){
        this.fileInfoCache = cache;
    }

    public void handleDownloadRequest(ChannelHandlerContext ctx, NetworkPacket packet){
        try{
            byte[] content = packet.getContent();
            String fileId = new String(content, SwarmConstants.DEFAULT_CHARSET);
            String path = fileInfoCache.getPath(fileId);
            FileInfo fileInfo = fileInfoCache.getFileInfo(fileId);
            if(fileInfo == null){
                throw new RuntimeException("file doesn't exist");
            }
            Serializer serializer = new ProtoStuffSerializer();

            byte[] headContent = serializer.serialize(fileInfo, FileInfo.class);

            NetworkPacket headPacket = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_HEAD, headContent);
            ctx.channel().writeAndFlush(headPacket);

            File file = new File(path);
            ShardedFileSender shardedFileSender = new ShardedFileSender(ctx.channel(), serializer, new DefaultFileTransferCallback());
            shardedFileSender.send(file, fileId);
            NetworkPacket endPacket = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_END, fileId.getBytes(SwarmConstants.DEFAULT_CHARSET));
            endPacket.setId(packet.getId());
            ctx.writeAndFlush(endPacket);
        }catch (Exception e){
            e.printStackTrace();
            NetworkPacket errorPacket = NetworkPacket.builder().type(PacketTypes.ERROR).id(packet.getId()).content(e.getMessage().getBytes(SwarmConstants.DEFAULT_CHARSET)).build();
            ctx.writeAndFlush(errorPacket);
        }

    }
}
