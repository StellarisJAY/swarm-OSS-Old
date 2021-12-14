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
 * @date 2021/12/14 20:13
 */
@Builder
@Getter
@ToString
public class FileUploadResponse {
    private String fileId;

    private List<StorageInfo> storageNodes;

}
