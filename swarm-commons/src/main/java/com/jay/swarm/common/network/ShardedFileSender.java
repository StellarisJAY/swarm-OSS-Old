package com.jay.swarm.common.network;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.network.callback.FileTransferCallback;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.Serializer;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;

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

    public void send(File file, String fileId){
        long totalSize = file.length();
        long sentLength = 0;
        try(FileInputStream inputStream = new FileInputStream(file)){
            FileChannel fileChannel = inputStream.getChannel();
            // 分配buffer，大小为文件ID大小（36） + 16字节MD5串 + 默认的分片大小
            ByteBuffer shardBuffer = ByteBuffer.allocate(fileId.length() + SwarmConstants.DEFAULT_SHARD_SIZE);
            // buffer中放入文件ID
            shardBuffer.put(fileId.getBytes(SwarmConstants.DEFAULT_CHARSET));
            int idEndPosition = shardBuffer.position();
            int length;
            while((length = fileChannel.read(shardBuffer)) != -1){
                // 已发送的大小
                sentLength += length;
                /*
                    需要优化，过多堆内外拷贝
                 */
                byte[] content = new byte[length + fileId.length()];
                shardBuffer.rewind();
                shardBuffer.get(content);
                NetworkPacket shardPacket = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_BODY, content);
                if(channel == null){
                    CompletableFuture<Object> future = client.sendAsync(shardPacket);
                }
                else{
                    channel.writeAndFlush(shardPacket);
                }
                float progress = new BigDecimal(sentLength * 100).divide(new BigDecimal(totalSize), 2, RoundingMode.HALF_DOWN).floatValue();
                callback.onProgress(fileId, totalSize, sentLength, progress);
                shardBuffer.clear();
                shardBuffer.position(idEndPosition);
            }
            fileChannel.close();
        }catch (Exception e){
            log.info("file shard sending error, {}", e.getMessage());
            if(log.isDebugEnabled()){
                log.debug("file shard sending error", e);
            }
        }
    }
}
