package com.jay.swarm.common.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/8
 **/
@Slf4j
public class PropertyUtil {
    private static Properties properties = new Properties();

    static {
        try(InputStream inputStream = PropertyUtil.class.getClassLoader().getResourceAsStream("namenode.properties")){
            properties.load(inputStream);
        }catch (IOException e){
            log.error("unable to load namenode.properties ", e);
        }
    }

    public static String get(String name){
        return properties.getProperty(name);
    }
}
