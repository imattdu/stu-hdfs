package com.matt.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author matt
 * @create 2021-08-06 1:41
 */
public class TestJunit {

    @Before
    public void init(){
        System.out.println("init");
    }

    @After
    public void close() throws IOException {
        System.out.println("close");
    }

    @Test
    public void test1() {
        System.out.println("run");
    }
}
