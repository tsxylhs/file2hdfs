package com.jn.file2hdfs.util;

import java.io.*;
import java.util.Properties;

/**
 * @ClassName PropertiesReader
 * @Author lvhoushuai(tsxylhs @ outlook.com)
 * @Date 2021/12/21
 **/
public class PropertiesReader {
    /**
     * 配置文件
     */
    private static Properties properties = new Properties();

    /**
     * 读取 properties 测试项目配置文件
     *
     * @param propertiesPath 配置文件路径
     * @return Properties
     * @throws IOException IOException
     */
    public static Properties readProperties(String propertiesPath) throws IOException {
        System.out.println("开始读取配置文件");
        InputStream inputStream = new FileInputStream(propertiesPath);
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "utf-8");
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        properties.load(bufferedReader);
        return properties;
    }

    /**
     * 依据键名获取配置文件中的键值
     *
     * @param key 键名
     * @return 键值
     */
    public static String getKey(String key) {
        return properties.getProperty(key);
    }

}
