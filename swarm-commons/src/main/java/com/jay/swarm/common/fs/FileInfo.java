package com.jay.swarm.common.fs;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
@Builder
@Getter
@ToString
public class FileInfo {
    private String fileId;

    private long totalSize;

    private byte[] md5;

    private int shardCount;
}
