package com.jay.swarm.overseer.config;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * <p>
 *  Overseer 配置
 * </p>
 *
 * @author Jay
 * @date 2021/12/10
 **/
@Slf4j
public class OverseerConfig {
    private final Properties properties;

    private final String propertyPath;

    public OverseerConfig(String path)  {
        propertyPath = path;
        properties = new Properties();
        try(InputStream inputStream = new FileInputStream(path)){
            properties.load(inputStream);
        }catch (Exception e){
            log.error("failed to load config for overseer", e);
            throw new RuntimeException("failed to load config for Overseer");
        }
    }

    public String getConfig(String name){
        return properties.getProperty(name);
    }
}
