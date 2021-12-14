package com.jay.swarm.common.network.callback;

/**
 * <p>
 *  文件传输回调
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
public interface FileTransferCallback {

    default void onProgress(String fileId, long total, long current, float progress){

    }

    default void onComplete(String fileId, long timeUsed, long size){

    }
}
