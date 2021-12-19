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

    @Override
    public void onProgress(String fileId, long total, long current, float progress) {
        log.debug("file {} transfer progress: {} %", fileId, progress);
    }

    @Override
    public void onComplete(String fileId, long timeUsed, long size) {
        log.debug("file {} transfer finished: time used {} ms, size: {}, speed: {}", fileId, timeUsed, formatSize(size), calculateSpeed(size, timeUsed));
    }

    private String formatSize(long size){
        BigDecimal s = new BigDecimal(size);
        if(size < 1024){
            return size + " bytes";
        }
        else if(size < 1024 * 1024){
            return s.divide(new BigDecimal(1024), 2, RoundingMode.HALF_DOWN).toString() + " KB";
        }
        else if(size < 1024 * 1024 * 1024){
            return s.divide(new BigDecimal(1024 * 1024), 2, RoundingMode.HALF_DOWN).toString() + " MB";
        }
        else{
            return s.divide(new BigDecimal(1024 * 1024 * 1024), 2, RoundingMode.HALF_DOWN).toString() + " GB";
        }
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
