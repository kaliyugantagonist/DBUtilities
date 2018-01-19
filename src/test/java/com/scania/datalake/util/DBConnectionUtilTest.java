package com.scania.datalake.util;

import org.junit.Assert;

import java.util.Properties;

import static org.junit.Assert.*;

public class DBConnectionUtilTest {
    @org.junit.Test
    public void getConnection() throws Exception {

        Properties properties = new Properties();
        properties.load(ClassLoader.getSystemResourceAsStream("dev-hive-properties"));

        Assert.assertNull(DBConnectionUtil.getConnection(properties));
    }

}