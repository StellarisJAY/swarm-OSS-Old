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
public class FileShard {
    private String fileId;
    private byte[] content;
}
