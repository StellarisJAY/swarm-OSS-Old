package com.jay.swarm.overseer.meta;

import com.jay.swarm.common.entity.MetaData;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.common.util.ScheduleUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/10
 **/
@Slf4j
public class Persistence {

    private final MetaDataManager metaDataManager;

    public Persistence(MetaDataManager metaDataManager) {
        this.metaDataManager = metaDataManager;
    }

    /**
     * 开启持久化任务
     * @param time 时间周期
     */
    public void init(long time){
        // 定时任务
        ScheduleUtil.scheduleAtFixedRate(()->{
            long perStart = System.currentTimeMillis();
            // 获取当前元数据缓存的快照副本
            List<MetaData> metaData = metaDataManager.copyOfCache();
            // 持久化副本
            metaDataPersistence(metaData);
            log.info("metadata saved, time used: {} ms", (System.currentTimeMillis() - perStart));
        }, time,  time, TimeUnit.MILLISECONDS);
    }

    /**
     * 持久化过程
     * @param metaData 元数据集合
     */
    private void metaDataPersistence(List<MetaData> metaData){
        File file = new File("D:/meta-data.dump");
        try(FileOutputStream outputStream = new FileOutputStream(file)){
            // channel
            FileChannel channel = outputStream.getChannel();
            // 序列化工具
            Serializer serializer = new ProtoStuffSerializer();
            // 每个metaData序列化后写入文件
            for(MetaData meta : metaData){
                // 序列化
                byte[] serialized = serializer.serialize(meta, MetaData.class);
                // 创建buffer，大小为序列化后字节数 + 换行符
                ByteBuffer buffer = ByteBuffer.allocate(serialized.length + 2);
                buffer.put(serialized);
                buffer.put((byte)'\r');
                buffer.put((byte)'\n');
                buffer.rewind();
                // 写入channel
                channel.write(buffer);
                buffer.clear();
            }
            channel.close();
        }catch (IOException e){
            log.error("metadata persistence failed: ", e);
        }
    }
}
