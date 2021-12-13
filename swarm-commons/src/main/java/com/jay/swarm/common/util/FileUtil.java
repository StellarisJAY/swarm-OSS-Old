package com.jay.swarm.common.util;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
public class FileUtil {
    public static byte[] md5(String path){
        try(FileInputStream inputStream = new FileInputStream(new File(path))){
            return DigestUtils.md5(inputStream);
        }catch (IOException e){
            e.printStackTrace();
            return null;
        }
    }
}
