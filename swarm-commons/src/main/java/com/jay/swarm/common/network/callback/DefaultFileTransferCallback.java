package com.jay.swarm.common.network.callback;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
@Slf4j
public class DefaultFileTransferCallback implements FileTransferCallback {

    private final String path;

    public DefaultFileTransferCallback(String path) {
        this.path = path;
    }

    @Override
    public String getFilePath() {
        return path;
    }

    @Override
    public void onProgress(String fileId, long total, long current, float progress) {

    }

    @Override
    public void onComplete(String fileId, long timeUsed, long size) {
        log.info("file {} transfer finished: time used {} ms, size: {} B, speed: {}", fileId, timeUsed, size, calculateSpeed(size, timeUsed));
    }

    private String calculateSpeed(long size, long time){
        double speed = (double)size * 1000 / time;
        BigDecimal s = new BigDecimal(speed);
        if(speed < 1024){
            return speed + " byte/s";
        }
        else if(speed < 1024 * 1024){
            return s.divide(new BigDecimal(1024), 2, RoundingMode.HALF_DOWN).toString() + " KB/s";
        }
        else{
            return s.divide(new BigDecimal(1024 * 1024), 2, RoundingMode.HALF_DOWN).toString() + " MB/s";
        }
    }
}