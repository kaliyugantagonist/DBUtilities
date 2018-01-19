package com.scania.datalake.util;

import com.scania.datalake.exception.InvalidDBPropertyException;

import java.io.IOException;
import java.util.Properties;

public class DBConnectionUtilTester {

    public static void main(String [] args) throws InvalidDBPropertyException, IOException {

        Properties properties = new Properties();
        properties.load(ClassLoader.getSystemResourceAsStream("test-dev-hive.properties"));
        DBConnectionUtil.getConnection(properties);
    }
}
