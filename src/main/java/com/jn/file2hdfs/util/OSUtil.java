package com.jn.file2hdfs.util;

import org.apache.commons.compress.compressors.FileNameUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * OS Utility Class This is used to obtain the os related var programmatically
 *
 * <p>
 * <a h ref="OSUtil.java.html"><i>View Source</i></a>
 * </p>
 *
 *   */
public class  OSUtil {

    public  static String  windows2linuxPath(String path){
        String s = path.replace(":\\", "/").replace("\\", "/");
        System.out.println(s);
        return s;

    }
    public static String linux2windowsPath(String path){
        String[] arrs = path.split("/");
        String v = path.substring(0, arrs[0].length()) + ":\\" + path.substring(arrs[0].length() + 1, path.length() - 1);
        String kv = v.replace("/", "\\");
        System.out.println(kv);
        return kv;
    }

}
