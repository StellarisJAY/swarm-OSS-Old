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
        String filename = encodeFileName(fileId);
        int hash = StringUtils.hash(filename, HASH_SIZE * HASH_SIZE);

        int parent = hash / HASH_SIZE;
        int child = hash % HASH_SIZE;

        return baseDir + File.separator + parent + File.separator + child + File.separator + fileId;
    }

    public abstract String encodeFileName(String fileId);
}
