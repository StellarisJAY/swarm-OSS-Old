package com.jay.swarm.common.fs;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * <p>
 *  文件信息实体
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
@Builder
@Getter
@ToString
public class FileInfo {
    /**
     * ID
     */
    private String fileId;

    /**
     * 大小
     */
    private long totalSize;

    /**
     * md5校验码
     */
    private byte[] md5;

    /**
     * 分片数量
     */
    private int shardCount;
}
