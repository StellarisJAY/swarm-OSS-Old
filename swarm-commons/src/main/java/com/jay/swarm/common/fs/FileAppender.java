package com.jay.swarm.common.fs;

import com.jay.swarm.common.network.callback.DefaultFileTransferCallback;
import com.jay.swarm.common.network.callback.FileTransferCallback;
import com.jay.swarm.common.util.FileUtil;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
@Slf4j
public class FileAppender {
    /**
     * 文件ID
     */
    private final String fileId;

    /**
     * 文件md5，用于传输校验
     */
    private final byte[] fileMd5;

    /**
     * 文件传输回调
     */
    private final FileTransferCallback transferCallback;

    /**
     * 文件输出通道
     */
    private final FileChannel fileChannel;
    /**
     * 目标文件总大小
     */
    private final long totalSize;

    /**
     * 接收到的大小
     */
    private long receivedSize;
    private final String path;

    private final long transferStartTime;

    public FileAppender(String fileId, String path, byte[] fileMd5, long totalSize) throws IOException {
        this.fileMd5 = fileMd5;
        this.totalSize = totalSize;
        this.fileId = fileId;
        this.path = path;
        this.transferCallback = new DefaultFileTransferCallback();
        File file = new File(path);

        // 创建文件的父目录
        File parent = file.getParentFile();
        if(!parent.exists() && !parent.mkdirs()){
            throw new RuntimeException("unable to create parent path");
        }
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = randomAccessFile.getChannel();

        this.transferStartTime = System.currentTimeMillis();
    }

    /**
     * 末尾添加数据
     * @param data 数据 byte[]
     * @throws IOException IOException
     */
    public void append(byte[] data) throws IOException {
        // wrap 数据
        ByteBuffer buffer = ByteBuffer.wrap(data);
        // 写入channel
        fileChannel.write(buffer);
        buffer.clear();
        // 计算接收大小和接收进度
        receivedSize += data.length;
        float progress = new BigDecimal(receivedSize).multiply(new BigDecimal(100))
                .divide(new BigDecimal(totalSize), 2, RoundingMode.HALF_DOWN)
                .floatValue();
        // 进度回调
        transferCallback.onProgress(fileId, totalSize, receivedSize, progress);
    }

    public void append(ByteBuf data) throws IOException {
        ByteBuffer buffer = data.nioBuffer();
        int length = fileChannel.write(buffer);
        // 计算接收大小和接收进度
        receivedSize += length;
        float progress = new BigDecimal(receivedSize).multiply(new BigDecimal(100))
                .divide(new BigDecimal(totalSize), 2, RoundingMode.HALF_DOWN)
                .floatValue();
        // 进度回调
        transferCallback.onProgress(fileId, totalSize, receivedSize, progress);
    }

    @Deprecated
    public void write(long position, byte[] data) throws IOException {
        MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, data.length);
        buffer.put(data);
        // 计算接收大小和接收进度
        receivedSize += data.length;
        float progress = new BigDecimal(receivedSize).multiply(new BigDecimal(100))
                .divide(new BigDecimal(totalSize), 2, RoundingMode.HALF_DOWN)
                .floatValue();
        // 进度回调
        transferCallback.onProgress(fileId, totalSize, receivedSize, progress);
    }


    /**
     * 完成传输
     */
    public void complete(){
        // 计算下载完成的文件md5
        byte[] md5 = FileUtil.md5(path);

        // 检查md5是否相等，判断文件在传输中是否损坏
        if(!Arrays.equals(md5, fileMd5)){
            log.info("original md5: {}", Arrays.toString(fileMd5));
            log.info("transfer md5: {}", Arrays.toString(md5));
            throw new IllegalStateException("file damaged during transfer, fileID: " + fileId);
        }
        // 触发传输完成回调
        transferCallback.onComplete(fileId, (System.currentTimeMillis() - transferStartTime), receivedSize);
        release();
    }


    /**
     * 释放FileAppender
     * 关闭channel和OutputStream
     */
    public void release(){
        if(fileChannel != null){
            try{
                fileChannel.close();
            }catch (IOException e){
                log.error("error when closing fileChannel", e);
            }
        }

    }
}
