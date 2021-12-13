package com.jay.swarm.common.fs.locator;

import com.jay.swarm.common.util.StringUtils;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
public class Md5FileLocator extends AbstractFileLocator {
    public Md5FileLocator(String baseDir) {
        super(baseDir);
    }

    @Override
    public String encodeFileName(String fileId) {
        return StringUtils.md5(fileId);
    }
}
