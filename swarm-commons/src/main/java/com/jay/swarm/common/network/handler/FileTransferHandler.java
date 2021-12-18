package com.jay.swarm.common.network.handler;

import com.jay.swarm.common.fs.FileAppender;
import com.jay.swarm.common.fs.FileInfo;
import com.jay.swarm.common.fs.FileInfoCache;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *  文件传输处理器
 *  负责处理从从网络传输到该节点的文件
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
@Slf4j
public class FileTransferHandler {
    /**
     * 文件拼接器Map
     * key：文件ID
     */
    private final Map<String, FileAppender> appenderMap = new HashMap<>(256);

    private final FileInfoCache fileInfoCache;

    public FileTransferHandler(){
        this.fileInfoCache = null;
    }

    public FileTransferHandler(FileInfoCache fileInfoCache) {
        this.fileInfoCache = fileInfoCache;
    }

    /**
     * 处理文件传输头
     * @param fileInfo 文件信息
     * @param path 存储路径
     * @throws IOException IOException
     */
    public void handleTransferHead(FileInfo fileInfo, String path) throws IOException {
        // 创建appender
        FileAppender fileAppender = new FileAppender(fileInfo.getFileId(), path, fileInfo.getMd5(), fileInfo.getTotalSize());
        appenderMap.put(fileInfo.getFileId(), fileAppender);
        // 缓存文件信息
        if(fileInfoCache != null){
            fileInfoCache.saveFileInfo(fileInfo);
        }
    }


    public void handleTransferBody(String fileId, byte[] content) throws IOException {
        // 添加分片到文件拼接器
        FileAppender appender = appenderMap.get(fileId);
        if(appender == null){
            throw new FileNotFoundException("no file appender found");
        }
        appender.append(content);
    }


    public void handleTransferEnd(String fileId){
        // 拼接器complete
        FileAppender appender = appenderMap.remove(fileId);
        appender.complete();
    }
}
