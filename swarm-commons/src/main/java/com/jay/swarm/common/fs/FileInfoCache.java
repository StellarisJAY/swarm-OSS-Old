package com.jay.swarm.common.fs;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.fs.locator.FileLocator;
import com.jay.swarm.common.util.FileUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/14 12:16
 */
@Slf4j
public class FileInfoCache {
    private final Map<String, FileInfo> fileInfoMap = new ConcurrentHashMap<>(256);
    private final Map<String, String> pathCache = new ConcurrentHashMap<>(256);

    private final FileLocator locator;

    public FileInfoCache(FileLocator locator) {
        this.locator = locator;
    }

    public FileInfo getFileInfo(String fileId){
        FileInfo fileInfo = fileInfoMap.get(fileId);
        if (fileInfo == null){
            synchronized (fileInfoMap){
                if((fileInfo = fileInfoMap.get(fileId)) == null){
                    String path = getPath(fileId);
                    File file = new File(path);
                    if(!file.exists() || file.isDirectory()){
                        return null;
                    }
                    byte[] md5 = FileUtil.md5(path);
                    fileInfo = FileInfo.builder().fileId(fileId)
                            .md5(md5)
                            .totalSize(file.length())
                            .shardCount((int) Math.round((double)file.length() / SwarmConstants.DEFAULT_SHARD_SIZE))
                            .build();
                    fileInfoMap.put(fileId, fileInfo);
                }
            }
        }
        return fileInfo;
    }

    public String getPath(String fileId){
        String path = pathCache.get(fileId);
        if(path == null){
            synchronized (pathCache){
                if((path = pathCache.get(fileId)) == null){
                    path = locator.locate(fileId);
                    pathCache.put(fileId, path);
                }
            }
        }
        return path;
    }

    public void saveFileInfo(FileInfo fileInfo){
        fileInfoMap.put(fileInfo.getFileId(), fileInfo);
    }
}
