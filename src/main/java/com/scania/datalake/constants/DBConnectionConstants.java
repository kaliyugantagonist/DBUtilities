package com.scania.datalake.constants;

public final class DBConnectionConstants {

    public static final String DB_VENDOR = "dbVendor";

    /*Start : Hive*/
    public static final String HIVE_DB_PROPERTIES_FILE = "db/hive-db.properties";
    //Basic
    public static final String HIVE_JDBC_URL = "JDBC_URL";
    public static final String HIVE_JDBC_URL_VALUE_SEPARATOR = "JDBC_URL_VALUE_SEPARATOR";
    public static final String HIVE_JDBC_URL_PROPERTY_SEPARATOR = "JDBC_URL_PROPERTY_SEPARATOR";
    public static final String HIVE_HOST = "host";
    public static final String HIVE_PORT = "port";
    public static final String HIVE_DB = "db";
    public static final String HIVE_CLUSTER_SECURITY_TYPE = "clusterSecurityType";
    public static final String HIVE_JDBC_CONNECT_WAY = "jdbcConnectWay";

    public enum HiveJDBCConnectWays {
        ZOOKEEPER_SERVICE_DISCOVERY, DIRECT;
    }

    public enum HiveClusterSecurityTypes {
        INSECURE, KERBERIZED, KERBERIZED_PREAUTH_SUBJECT;
    }

    //Zookeeper Service Discovery
    public static final String HIVE_ZOOKEEPER_CONNECT_STRING = "zookeeper.connectString";
    public static final String HIVE_SERVICE_DISCOVERY_MODE = "serviceDiscoveryMode";
    public static final String HIVE_ZOOKEEPER_NAMESPACE = "zooKeeperNamespace";
    //Communicate with a kerberized cluster
    public static final String HIVE_PRINCIPAL = "principal";
    public static final String HIVE_PREAUTH_SUBJECT_KERBEROS_AUTH_TYPE = "PREAUTH_SUBJECT_KERBEROS_AUTH_TYPE";
    public static final String HIVE_PREAUTH_SUBJECT_AUTH = "PREAUTH_SUBJECT_AUTH";
    //Other properties
    public static final String HIVE_TRANSPORT_MODE = "transportMode";
    public static final String HIVE_HTTP_PATH = "httpPath";
    //Documentation
    public static final String HIVE_WIKI_JDBC_WEBPAGE = "HIVE_WIKI_JDBC_WEBPAGE";
    public static final String HIVE_JDBC_URL_DBNAME_SEPARATOR = "JDBC_URL_DBNAME_SEPARATOR";
    /*End : Hive*/
}
