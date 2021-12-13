package com.jay.swarm.common.fs.locator;

/**
 * <p>
 *  文件定位接口，使用不同的规则根据ID找到文件
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
public interface FileLocator {

    /**
     * 通过文件ID定位文件位置
     * @param fileId id
     * @return 文件路径
     */
    String locate(String fileId);
}
