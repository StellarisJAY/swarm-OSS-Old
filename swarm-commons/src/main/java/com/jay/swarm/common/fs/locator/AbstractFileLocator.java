package com.jay.swarm.common.fs.locator;

import com.jay.swarm.common.util.StringUtils;

import java.io.File;

/**
 * <p>
 *  文件定位器抽象
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
public abstract class AbstractFileLocator implements FileLocator {

    private final String baseDir;
    private static final int HASH_SIZE = 256;

    public AbstractFileLocator(String baseDir) {
        this.baseDir = baseDir;
    }

    @Override
    public String locate(String fileId) {
        // 编码文件名
        String filename = encodeFileName(fileId);
        // 用文件名计算hash
        int hash = StringUtils.hash(filename, HASH_SIZE * HASH_SIZE);

        // hash / 最大值 作为一级目录
        int parent = hash / HASH_SIZE;
        // hash % 最大值 作为二级目录
        int child = hash % HASH_SIZE;

        return baseDir + File.separator + parent + File.separator + child + File.separator + fileId;
    }

    /**
     * 文件名加密方法
     * 将文件ID编码后作为文件名
     * @param fileId 文件ID
     * @return String 文件名
     */
    public abstract String encodeFileName(String fileId);
}
