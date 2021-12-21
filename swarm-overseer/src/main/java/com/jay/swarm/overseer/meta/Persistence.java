package com.jay.swarm.overseer.meta;

import com.jay.swarm.common.config.Config;
import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.Bucket;
import com.jay.swarm.common.entity.MetaData;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.common.util.ScheduleUtil;
import com.jay.swarm.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
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
            // 持久化副本
            metaDataPersistence(finalFilePath);
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
            try(FileInputStream inputStream = new FileInputStream(file);
                FileChannel channel = inputStream.getChannel()){
                // 读取整个文件的buffer
                ByteBuffer buffer = ByteBuffer.allocateDirect((int) file.length());
                channel.read(buffer);
                byte[] bucketIdBytes = new byte[16];
                while(buffer.hasRemaining()){
                    // 读取桶ID
                    buffer.get(bucketIdBytes);
                    String bucketId = new String(bucketIdBytes, SwarmConstants.DEFAULT_CHARSET);
                    if(StringUtils.isEmpty(bucketId)){
                        throw new RuntimeException("bucket head format error");
                    }
                    // 读取桶中元数据个数
                    int metaSize = buffer.getInt();
                    // 读取桶信息
                    Bucket bucket = loadBucketInfo(buffer, bucketId, metaSize);
                    // 保存桶信息
                    metaDataManager.saveBucket(bucket);
                    // 加载元数据
                    if(metaSize > 0){
                        loadMetaData(buffer, metaSize, bucket);
                    }
                }
            }catch (RuntimeException e){
                log.warn("load persistence error", e);
            }
            catch (IOException e){
                throw new RuntimeException("unable to load persistence file");
            }
        }
        return loaded;
    }


    /**
     * 读取桶信息
     * @param buffer buffer
     * @param bucketId 桶ID
     * @param metaSize 元数据数量
     * @return Bucket
     */
    private Bucket loadBucketInfo(ByteBuffer buffer, String bucketId, int metaSize){
        // 读取桶信息部分数据长度
        int infoLength = buffer.getInt();
        // 长度必须大于18，18 = ownerId 8 + access 2 + time 8
        if(infoLength > 18){
            // 读取桶信息
            long ownerId = buffer.getLong();
            short accessibility = buffer.getShort();
            long createTime = buffer.getLong();
            byte[] bucketName = new byte[infoLength - 20];
            buffer.get(bucketName);

            return Bucket.builder().id(bucketId)
                    .ownerId(ownerId)
                    .createTime(createTime)
                    .accessibility(accessibility)
                    .name(new String(bucketName, SwarmConstants.DEFAULT_CHARSET))
                    .objectIds(new ArrayList<>(metaSize)).build();
        }else{
            // 长度错误
            throw new RuntimeException("bucket data format error");
        }
    }

    /**
     * 加载元数据
     * @param buffer buffer
     * @param metaSize 元数据数量
     * @param bucket 桶
     */
    private void loadMetaData(ByteBuffer buffer, int metaSize, Bucket bucket){
        for(int i = 0; i < metaSize; i++){
            // 元数据长度
            int length = buffer.getInt();
            if(length > 0){
                // 读取序列化的元数据
                byte[] serialized = new byte[length];
                buffer.get(serialized);
                // 反序列化
                MetaData metaData = serializer.deserialize(serialized, MetaData.class);
                // 判断是否是空数据
                if(StringUtils.isEmpty(metaData.getKey()) || StringUtils.isEmpty(metaData.getFilename())){
                    throw new RuntimeException("empty meta data");
                }
                // 记录到metaDataManger和bucket
                metaDataManager.putMetaData(metaData);
                bucket.getObjectIds().add(metaData.getKey());
            }else{
                throw new RuntimeException("meta data format error");
            }
        }
    }

    /**
     * 持久化过程
     * @param path 持久化路径
     */
    private int metaDataPersistence(String path){
        File file = new File(path);
        // 获取所有的桶
        List<Bucket> buckets = metaDataManager.listBuckets();
        int savedMeta = 0;
        try(FileOutputStream outputStream = new FileOutputStream(file);
            FileChannel channel = outputStream.getChannel()){

            for (Bucket bucket : buckets) {
                if(StringUtils.isEmpty(bucket.getId()) || StringUtils.isEmpty(bucket.getName())){
                    continue;
                }
                // 持久化桶信息
                bucketHeadPersistence(bucket, channel);
                bucketInfoPersistence(bucket, channel);
                // 持久化元数据
                if(bucket.getObjectIds() != null && !bucket.getObjectIds().isEmpty()){
                    // 获取元数据集合
                    List<MetaData> metaDatas = metaDataManager.copyOfCache(bucket.getObjectIds());
                    // 遍历元数据
                    for (MetaData metaData : metaDatas) {
                        // 序列化元数据
                        byte[] serialized = serializer.serialize(metaData, MetaData.class);
                        // 元数据长度
                        int length = serialized.length;
                        // 写入长度 + 数据
                        ByteBuffer buffer = ByteBuffer.allocate(length + 4);
                        buffer.putInt(length);
                        buffer.put(serialized);
                        buffer.rewind();
                        channel.write(buffer);
                        buffer.clear();
                        savedMeta++;
                    }
                }
            }
        }catch (IOException e){
            log.error("metadata persistence failed: ", e);
        }
        return savedMeta;
    }

    /**
     * 持久化桶头部信息
     * @param bucket 桶
     * @param channel channel
     * @throws IOException IOException
     */
    private void bucketHeadPersistence(Bucket bucket, FileChannel channel) throws IOException {
        // 获取桶中的元数据ID
        List<String> metaIds = bucket.getObjectIds();

        // 桶信息头部
        ByteBuffer bucketHeadBuffer = ByteBuffer.allocate(20);
        // 写入16字节桶ID
        bucketHeadBuffer.put(bucket.getId().getBytes(SwarmConstants.DEFAULT_CHARSET));
        // 写入4字节int，元数据个数
        bucketHeadBuffer.putInt(metaIds.size());
        bucketHeadBuffer.rewind();
        channel.write(bucketHeadBuffer);

    }

    /**
     * 持久化桶信息
     * @param bucket 桶
     * @param channel channel
     * @throws IOException IOException
     */
    private void bucketInfoPersistence(Bucket bucket, FileChannel channel) throws IOException {
        // 桶信息
        byte[] bucketName = bucket.getName().getBytes(SwarmConstants.DEFAULT_CHARSET);
        long bucketCreateTime = bucket.getCreateTime();
        ByteBuffer bucketInfoBuffer = ByteBuffer.allocate(18 + bucketName.length);
        // 信息长度 = ownerId 8字节 + 访问权限 2字节 +  创建时间戳8字节 + 桶名字
        bucketInfoBuffer.putInt(8 + bucketName.length);
        bucketInfoBuffer.putLong(bucket.getOwnerId());
        bucketInfoBuffer.putShort(bucket.getAccessibility());
        bucketInfoBuffer.putLong(bucketCreateTime);
        bucketInfoBuffer.put(bucketName);
        bucketInfoBuffer.rewind();
        channel.write(bucketInfoBuffer);
    }

    /**
     * 添加进程关闭时自动持久化
     * @param filePath 文件路径
     */
    private void addShutdownPersistence(String filePath){
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            long perStart = System.currentTimeMillis();
            // 持久化副本
            int saved = metaDataPersistence(filePath);
            log.info("{} metadata saved, time used: {} ms", saved, (System.currentTimeMillis() - perStart));
        }));
    }

}
