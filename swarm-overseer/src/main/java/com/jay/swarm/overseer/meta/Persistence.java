package com.jay.swarm.overseer.meta;

import com.jay.swarm.common.config.Config;
import com.jay.swarm.common.entity.MetaData;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.common.util.AppendableByteArray;
import com.jay.swarm.common.util.ScheduleUtil;
import com.jay.swarm.common.util.StringUtils;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  持久化工具
 *  目前的持久化使用序列化+写入文件方式
 *  经过测试：10w条缓存数据的持久化停顿时间为500ms左右。
 *  准备加入类似Redis的AOF持久化方式，减轻持久化过程、提高实时性。
 * </p>
 *
 * @author Jay
 * @date 2021/12/10
 **/
@Slf4j
public class Persistence {

    private final MetaDataManager metaDataManager;
    /**
     * 默认的持久化周期10s，强烈建议根据实际情况配置持久化周期，避免文件信息丢失
     */
    private static final long DEFAULT_PERSISTENCE_PERIOD = 10000;
    /**
     * 默认的持久化文件
     */
    private static final String DEFAULT_PERSISTENCE_PATH = "D:/swarm/meta-data.dump";
    private final Config config;
    private final Serializer serializer;


    public Persistence(MetaDataManager metaDataManager, Config config, Serializer serializer) {
        this.metaDataManager = metaDataManager;
        this.config = config;
        this.serializer = serializer;
    }

    /**
     * 开启持久化任务
     */
    public void init(){
        long initStart = System.currentTimeMillis();
        long time;
        // 读取持久化周期
        String period = config.get("persistence.time");
        if(StringUtils.isEmpty(period) || !period.matches("^[0-9]*$")){
            time = DEFAULT_PERSISTENCE_PERIOD;
        }else{
            time = Long.parseLong(period);
        }
        String filePath = config.get("persistence.path");
        if(filePath == null){
            filePath = DEFAULT_PERSISTENCE_PATH;
        }
        // 启动时读取持久化文件
        int countMeta = loadPersistence(filePath);
        // 定时任务
        String finalFilePath = filePath;
        ScheduleUtil.scheduleAtFixedRate(()->{
            long perStart = System.currentTimeMillis();
            // 获取当前元数据缓存的快照副本
            List<MetaData> metaData = metaDataManager.copyOfCache();
            // 持久化副本
            metaDataPersistence(metaData, finalFilePath);
            log.debug("metadata saved, time used: {} ms", (System.currentTimeMillis() - perStart));
        }, time,  time, TimeUnit.MILLISECONDS);
        addShutdownPersistence(filePath);
        log.info("persistence init finished, loaded {} meta-data time used: {} ms", countMeta, (System.currentTimeMillis() - initStart));
    }

    /**
     * 加载持久化数据
     * @param path 持久化文件路径
     */
    private int loadPersistence(String path){
        File file = new File(path);
        int loaded = 0;
        // 文件存在，如果不存在表示是第一次启动，不用加载数据
        if(file.exists() && !file.isDirectory() && file.length() != 0){
            try(FileInputStream inputStream = new FileInputStream(file)){
                // channel
                FileChannel channel = inputStream.getChannel();
                // 分配buffer，大小为文件大小
                ByteBuffer buffer = ByteBuffer.allocate((int)file.length());
                int length = channel.read(buffer);
                buffer.rewind();
                while (buffer.hasRemaining()){
                    int len = buffer.getInt();
                    byte[] serialized = new byte[len];
                    buffer.get(serialized);
                    MetaData metaData = serializer.deserialize(serialized, MetaData.class);
                    metaDataManager.putMetaData(metaData);
                    loaded++;
                }
                channel.close();
                buffer.clear();
            }catch (IOException e){
                throw new RuntimeException("unable to load persistence file");
            }
        }
        return loaded;
    }

    /**
     * 持久化过程
     * @param metaData 元数据集合
     */
    private int metaDataPersistence(List<MetaData> metaData, String path){
        File file = new File(path);
        int savedMeta = 0;
        try(FileOutputStream outputStream = new FileOutputStream(file)){
            // channel
            FileChannel channel = outputStream.getChannel();
            // 每个metaData序列化后写入文件
            for(MetaData meta : metaData){
                // 序列化
                byte[] serialized = serializer.serialize(meta, MetaData.class);
                // 创建buffer，大小为序列化后字节数 + 换行符
                ByteBuffer buffer = ByteBuffer.allocate(serialized.length + 4);
                buffer.putInt(serialized.length);
                buffer.put(serialized);
                buffer.rewind();
                // 写入channel
                channel.write(buffer);
                buffer.clear();
                savedMeta++;
            }
            channel.close();
        }catch (IOException e){
            log.error("metadata persistence failed: ", e);
        }
        return savedMeta;
    }

    private void addShutdownPersistence(String filePath){
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            long perStart = System.currentTimeMillis();
            // 获取当前元数据缓存的快照副本
            List<MetaData> metaData = metaDataManager.copyOfCache();
            // 持久化副本
            int saved = metaDataPersistence(metaData, filePath);
            log.info("{} metadata saved, time used: {} ms", saved, (System.currentTimeMillis() - perStart));
        }));
    }

}
