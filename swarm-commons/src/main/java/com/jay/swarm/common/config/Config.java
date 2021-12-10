package com.jay.swarm.common.config;

import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/10
 **/
@Slf4j
public class Config {
    private final Properties properties;

    public Config(String path) {
        this.properties = new Properties();
        try(InputStream inputStream = new FileInputStream(path)){
            properties.load(inputStream);
        }catch (Exception e){
            log.error("failed to load config from file " + path, e);
            throw new RuntimeException("failed to load config");
        }
    }

    public String get(String name){
        return properties.getProperty(name);
    }
}
