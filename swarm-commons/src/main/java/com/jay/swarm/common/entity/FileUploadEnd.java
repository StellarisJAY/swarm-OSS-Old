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
 * @date 2021/12/15 16:30
 */
@Builder
@ToString
@Getter
public class FileUploadEnd {
    private String fileId;
    private List<StorageInfo> otherStorages;
}
