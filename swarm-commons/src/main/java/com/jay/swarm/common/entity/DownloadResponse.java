package com.jay.swarm.common.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/16 11:12
 */
@Builder
@Getter
@ToString
public class DownloadResponse {
    private String fileId;
    private byte[] md5;
    private long size;

    private List<StorageInfo> storages;
}
