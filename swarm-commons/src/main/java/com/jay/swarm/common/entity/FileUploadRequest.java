package com.jay.swarm.common.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/14 19:59
 */
@Builder
@Getter
@ToString
public class FileUploadRequest {
    private String filename;
    private long size;
    private byte[] md5;
    private int backupCount;
}
