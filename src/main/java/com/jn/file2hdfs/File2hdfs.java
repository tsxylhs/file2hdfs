package com.jn.file2hdfs;

import com.jn.file2hdfs.util.OSUtil;
import com.jn.file2hdfs.util.PropertiesReader;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @ClassName file2hdfs
 * @Author lvhoushuai(tsxylhs @ outlook.com)
 * @Date 2021/12/21
 **/
public class File2hdfs {
    static Configuration conf;
    static Boolean SystemTag = true;

//    static {
//        conf=new Configuration();
//        conf.set("fs.defaultFS", "hdfs://192.168.121.133:9000");
//    }

    public static void main(String[] args) throws IOException {
        Properties properties=    PropertiesReader.readProperties("src/main/resources/config.properties");
        System.out.println(properties.get("hdfs.url"));
        System.out.println(properties.get("file.input"));
        System.out.println(properties.get("file.output"));
        FileSystem fs = FileSystem.get(conf);
       Map<String,String> source2target= FileToMap(properties.get("file.input").toString());
       if (!source2target.isEmpty()) {
           for (Map.Entry<String, String> m : source2target.entrySet()) {
               upload(fs, m.getKey(), m.getValue());
           }
       }
    }

    public static void upload(FileSystem fs,String source,String target) throws IOException {
        Path src = new Path(source);
        Path dest = new Path(target);
        fs.copyFromLocalFile(src, dest);
        FileStatus[] fileStatuses = fs.listStatus(dest);
        for (FileStatus file : fileStatuses) {
            System.out.println(file.getPath());
        }
        System.out.println("上传成功");
    }

    public static void MkdirHdfs() throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("/testPath"));
        fs.close();
        System.out.println("end");

    }

    public static Map<String, String> FileToMap(String path) {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        List<String> sourcePath = getAllFile(path);
        if (sourcePath.size() <= 0) {
            return null;
        }
        Map<String, String> map = new HashMap<>();
        for (String sourceStr : sourcePath) {
            map.put(sourceStr,OSUtil.windows2linuxPath(sourceStr));
        }
        return map;

    }
    public static List<String> getAllFile(String directoryPath) {
        LinkedList list = new LinkedList();
        if (directoryPath == null) {
            return list;
        }
        File baseFiles = new File(directoryPath);
        if (baseFiles == null || baseFiles.isFile() || !baseFiles.exists()) {
            return list;
        }
        File[] files = baseFiles.listFiles();
        if (files == null) {
            return list;
        }
        for (File file : files) {
            if (file == null) {
                continue;
            }
            if (file.isDirectory()) {
                list.addAll(getAllFile(file.getAbsolutePath()));
            } else {
                list.add(file.getAbsolutePath());
            }
        }
        return list;
    }
}
