package com.jn.file2hdfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName file2hdfs
 * @Author lvhoushuai(tsxylhs @ outlook.com)
 * @Date 2021/12/21
 **/
public class File2hdfs {
    static Configuration conf;
    static Boolean SystemTag=true;
//
//    static {
//        conf.set("fs.defaultFS", "hdfs://192.168.121.133:9000");
//    }

    public static void main(String[] args) {
        String sourceStr="D:\\java\\src.zip";
       String tmp= sourceStr.replace("/","://\"").replaceAll("/","//\"");
       System.out.println(tmp);

    }

    public static void upload() throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path("");
        Path dest = new Path("");
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
         if (sourcePath.size()<=0){
          return null;
        }
         Map<String,String> map=new HashMap<>();
         for(String sourceStr:sourcePath){
           sourceStr.replace("://\"","/").replaceAll("//\"","/");
         }
        return null;

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

    //路径转换
    public static String convert2linux(String path){
        if(SystemTag){
            int index=path.indexOf(":");
            if (index>1||index==-1){
                return path;
            }
            //Sysstor
        }
        return "";
    }
}
