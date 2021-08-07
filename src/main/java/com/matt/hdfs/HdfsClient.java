package com.matt.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author matt
 * @create 2021-08-06 1:20
 */
public class HdfsClient {

    FileSystem fs = null;

    // 1.获取客户端资源
    // 2.执行相应的命令
    // 3.关闭资源


    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();

        fs = FileSystem.get(new URI("hdfs://matt05:8020"), configuration, "matt");
    }

    @After
    public void close() throws IOException {
        fs.close();
    }


    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {
        fs.mkdirs(new Path("/test"));
    }

    // 配置文件
    //（1）客户端代码中设置的值
    // >（2）ClassPath 下的用户自定义配置文件
    //>（3）然后是服务器的自定义配置（xxx-site.xml）
    // >（4）服务器的默认配置（xxx-default.xml）
    @Test
    public void testPut() throws IOException {
        // 删除本地 覆盖文件
        fs.copyFromLocalFile(false,true,new Path("D:/var/swk.txt"), new Path("/xiyou"));
    }


    /**
     * 功能：
     * @author matt
     * @date 2021/8/6
     * @param
     * @return void
    */
    @Test
    public void testGet() throws IOException {
        // 4开启本地文件校验
        fs.copyToLocalFile(false,new Path("/xiyou/swk.txt"), new Path("D:/"), true);
    }


    @Test
    public void testRm() throws IOException {
        // 2:是否递归删除
        fs.delete(new Path("/sanguo"), true);
    }


    @Test
    public void testMv() throws IOException {
       // fs.rename(new Path("/test1"), new Path("/test1"));
       fs.rename(new Path("/test1"), new Path("/xiyou/"));
    }


    @Test
    public void testFileDetail() throws IOException {
        // test
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/xiyou"), false);
        while (remoteIterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = remoteIterator.next();
            System.out.println(Arrays.toString(locatedFileStatus.getBlockLocations()));
            System.out.println(locatedFileStatus.getOwner());
        }
    }


    @Test
    public void testIsFileOrDir() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.isFile());
            System.out.println(fileStatus.isDirectory());
            System.out.println(fileStatus.getPath().getName());
            System.out.println("-----------------------");
        }
    }
}
