package com.jay.swarm.common.fs;

import com.jay.swarm.common.fs.locator.FileLocator;
import com.jay.swarm.common.fs.locator.Md5FileLocator;
import com.jay.swarm.common.network.callback.DefaultFileTransferCallback;
import com.jay.swarm.common.network.callback.FileTransferCallback;
import com.jay.swarm.common.util.FileUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
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
    private final String fileId;

    private final byte[] fileMD5;

    private final FileTransferCallback transferCallback;

    private FileOutputStream outputStream;



    private final FileChannel fileChannel;
    /**
     * 目标文件总大小
     */
    private final long totalSize;

    /**
     * 接收到的大小
     */
    private long receivedSize;

    /**
     * 分片个数
     */
    private int shardCount;

    private long transferStartTime;

    public FileAppender(String fileId, String path, byte[] fileMD5, long totalSize, int shardCount) throws IOException {
        this.fileMD5 = fileMD5;
        this.totalSize = totalSize;
        this.shardCount = shardCount;
        this.fileId = fileId;

        this.transferCallback = new DefaultFileTransferCallback(path);
        File file = new File(path);

        File parent = file.getParentFile();
        boolean mk = parent.mkdirs();
        this.outputStream = new FileOutputStream(file);
        this.fileChannel = outputStream.getChannel();
        this.fileChannel.position(0);
        this.transferStartTime = System.currentTimeMillis();
    }

    public void append(byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        fileChannel.write(buffer);
        buffer.clear();
        receivedSize += data.length;
        float progress = new BigDecimal(receivedSize).multiply(new BigDecimal(100))
                .divide(new BigDecimal(totalSize), 2, RoundingMode.HALF_DOWN)
                .floatValue();
        transferCallback.onProgress(fileId, totalSize, receivedSize, progress);
    }

    public void complete(){
        // 计算下载完成的文件md5
        String path = transferCallback.getFilePath();
        byte[] md5 = FileUtil.md5(path);
        log.info("original md5: {}", Arrays.toString(fileMD5));
        log.info("transfer md5: {}", Arrays.toString(md5));
        // 检查md5是否相等，判断文件在传输中是否损坏
        if(!Arrays.equals(md5, fileMD5)){
            throw new IllegalStateException("file damaged during transfer, fileID: " + fileId);
        }
        // 触发传输完成回调
        transferCallback.onComplete(fileId, (System.currentTimeMillis() - transferStartTime), receivedSize);
    }


    public void release(){
        if(fileChannel != null){
            try{
                fileChannel.close();
            }catch (IOException e){
                log.error("error when closing fileChannel", e);
            }
        }
        if(outputStream != null){
            try{
                outputStream.close();
            }catch (IOException e){
                log.error("error when closing outputStream", e);
            }
        }
    }
}
