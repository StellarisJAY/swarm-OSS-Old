package com.jay.swarm.common.network;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.network.callback.FileTransferCallback;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.common.util.StringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * <p>
 *  文件分片发送器
 *  将文件分片后发送
 * </p>
 *
 * @author Jay
 * @date 2021/12/14 15:17
 */
@Slf4j
public class ShardedFileSender {
    private final Channel channel;

    private final BaseClient client;

    private final Serializer serializer;

    private final FileTransferCallback callback;

    public ShardedFileSender(Channel channel, Serializer serializer, FileTransferCallback callback) {
        this.channel = channel;
        this.callback = callback;
        this.client = null;
        this.serializer = serializer;
    }

    public ShardedFileSender(BaseClient client, Serializer serializer, FileTransferCallback callback) {
        this.client = client;
        this.callback = callback;
        this.channel = null;
        this.serializer = serializer;
    }

    public void send(File file,  String fileId){
        send(null, 0, file, fileId);
    }

//    public void send(String host, int port, File file, String fileId){
//        long totalSize = file.length();
//        long sentLength = 0;
//        try(FileInputStream inputStream = new FileInputStream(file)){
//            FileChannel fileChannel = inputStream.getChannel();
//            // 分配buffer，大小为文件ID大小（36） + 16字节MD5串 + 默认的分片大小
//            ByteBuffer shardBuffer = ByteBuffer.allocate(fileId.length() + SwarmConstants.DEFAULT_SHARD_SIZE);
//            // buffer中放入文件ID
//            shardBuffer.put(fileId.getBytes(SwarmConstants.DEFAULT_CHARSET));
//            int idEndPosition = shardBuffer.position();
//            int length;
//            while((length = fileChannel.read(shardBuffer)) != -1){
//                // 已发送的大小
//                sentLength += length;
//                /*
//                    需要优化，过多堆内外拷贝
//                 */
//                byte[] content = new byte[length + fileId.length()];
//                shardBuffer.rewind();
//                shardBuffer.get(content);
//                NetworkPacket shardPacket = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_BODY, content);
//                if(client != null && host != null){
//                    client.sendAsync(host, port, shardPacket);
//                }else if(channel != null){
//                    channel.writeAndFlush(shardPacket);
//                }
//                float progress = new BigDecimal(sentLength * 100).divide(new BigDecimal(totalSize), 2, RoundingMode.HALF_DOWN).floatValue();
//                callback.onProgress(fileId, totalSize, sentLength, progress);
//                shardBuffer.clear();
//                shardBuffer.position(idEndPosition);
//            }
//            fileChannel.close();
//        }catch (Exception e){
//            log.info("file shard sending error, {}", e.getMessage());
//            if(log.isDebugEnabled()){
//                log.debug("file shard sending error", e);
//            }
//        }
//    }

    public void send(String host, int port, File file, String fileId){
        long size = file.length();
        try(FileInputStream inputStream = new FileInputStream(file); FileChannel fileChannel = inputStream.getChannel()){
            int shardIndex = 0;
            while(size > 0){
                ByteBuffer data = ByteBuffer.allocate(SwarmConstants.DEFAULT_SHARD_SIZE + fileId.length());
                data.put(fileId.getBytes(SwarmConstants.DEFAULT_CHARSET));
                int length = fileChannel.read(data);
                data.flip();
                ByteBuf dataBuf = Unpooled.wrappedBuffer(data);
                if(client != null && !StringUtils.isEmpty(host)){
                    client.sendAsync(host, port, PacketTypes.TRANSFER_FILE_BODY, dataBuf);
                }else if(channel != null){
                    ByteBuf packet = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_BODY, dataBuf);
                    channel.writeAndFlush(packet);
                }
                size -= length;
            }
        }catch (IOException e){
            log.error("file shard sending error: ", e);
        }
    }
}
