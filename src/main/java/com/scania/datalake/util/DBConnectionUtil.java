package com.scania.datalake.util;

import com.scania.datalake.constants.DBConnectionConstants;
import com.scania.datalake.exception.InvalidDBPropertyException;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

public enum DBConnectionUtil {

    INSTANCE;

    //TODO : Configure the logger to output the qualified_class_name:method_name::
    private static org.apache.logging.log4j.Logger logger = LogManager.getLogger(DBConnectionUtil.class.getName());

    public static Connection getConnection(Properties properties) throws InvalidDBPropertyException {

        /*Start : Basic validation*/
        if (properties == null || properties.isEmpty()) {
            throw new InvalidDBPropertyException("Properties cannot be empty");
        }
        if (properties.getProperty(DBConnectionConstants.DB_VENDOR) == null || properties.getProperty(DBConnectionConstants.DB_VENDOR).isEmpty()) {
            throw new InvalidDBPropertyException("'dbVendor' cannot be empty");
        }
        if (properties.getProperty(DBConnectionConstants.HIVE_JDBC_CONNECT_WAY) == null || properties.getProperty(DBConnectionConstants.HIVE_JDBC_CONNECT_WAY).isEmpty()) {
            throw new InvalidDBPropertyException("'jdbcConnectWay' cannot be empty");
        }
        if (properties.getProperty(DBConnectionConstants.HIVE_CLUSTER_SECURITY_TYPE) == null || properties.getProperty(DBConnectionConstants.HIVE_CLUSTER_SECURITY_TYPE).isEmpty()) {
            throw new InvalidDBPropertyException("'clusterSecurityType' cannot be empty");
        }

        /*End : Basic validation*/
        Connection connection = null;

        String dbVendor = properties.getProperty(DBConnectionConstants.DB_VENDOR);

        switch (DBVendor.valueOf(dbVendor.toUpperCase())) {

            case HIVE: {

                return HiveConnectionUtil.INSTANCE.getConnection(properties);
            }

            case SQLSERVER: {
                logger.error("Connection retrieval not implemented");
                throw new UnsupportedOperationException("Connection retrieval for SQLSERVER is not implemented, yet");
            }

            case ORACLE: {
                logger.error("Connection retrieval not implemented");
                throw new UnsupportedOperationException("Connection retrieval for ORACLE is not implemented, yet");
            }
            case PHOENIX: {
                logger.error("Connection retrieval not implemented");
                throw new UnsupportedOperationException("Connection retrieval for PHOENIX is not implemented, yet");
            }

            default: {
                logger.error("Please specify a recognized vendor");
                throw new UnsupportedOperationException("Please specify a recognized vendor");
            }
        }
    }

    //TODO : create a method that retrieves the required values from the HIVE_CLIENT files viz. hive-site.xml

    private enum HiveConnectionUtil {

        INSTANCE;
        private Properties hiveDBProperties;

        {
            hiveDBProperties = new Properties();
            try {
                hiveDBProperties.load(ClassLoader.getSystemResourceAsStream(DBConnectionConstants.HIVE_DB_PROPERTIES_FILE));
            } catch (IOException e) {
                logger.error("Couldn't load the DB properties for Hive", e);
                throw new UnsupportedOperationException("Connection retrieval for Hive is not implemented, yet", e);
            }
        }


        public Connection getConnection(Properties properties) throws InvalidDBPropertyException {
            Connection connection = null;
            //TODO : Sanity checks

            String jdbcConnectWay = properties.getProperty(DBConnectionConstants.HIVE_JDBC_CONNECT_WAY);

            String clusterSecurityType = properties.getProperty(DBConnectionConstants.HIVE_CLUSTER_SECURITY_TYPE);

            StringBuffer jdbcConnectionStringBuffer = null;

            /*Start : JDBC connection ways*/
            if (jdbcConnectWay.equalsIgnoreCase(DBConnectionConstants.HiveJDBCConnectWays.ZOOKEEPER_SERVICE_DISCOVERY.toString())) {

                if (properties.getProperty(DBConnectionConstants.HIVE_ZOOKEEPER_CONNECT_STRING) == null || properties.getProperty(DBConnectionConstants.HIVE_ZOOKEEPER_CONNECT_STRING).isEmpty()) {
                    throw new InvalidDBPropertyException("Zookeeper connect string cannot be empty");
                }
                //TODO : Check if the ZK string is in the expected format(host names, port no.s etc.)
                jdbcConnectionStringBuffer = new StringBuffer(String.format(hiveDBProperties.getProperty(DBConnectionConstants.HIVE_JDBC_URL), properties.getProperty(DBConnectionConstants.HIVE_ZOOKEEPER_CONNECT_STRING)));

                if (properties.getProperty(DBConnectionConstants.HIVE_SERVICE_DISCOVERY_MODE) == null || properties.getProperty(DBConnectionConstants.HIVE_SERVICE_DISCOVERY_MODE).isEmpty()) {
                    throw new InvalidDBPropertyException("serviceDiscoveryMode cannot be empty");
                }
                jdbcConnectionStringBuffer.append(DBConnectionConstants.HIVE_SERVICE_DISCOVERY_MODE + "=" + properties.getProperty(DBConnectionConstants.HIVE_SERVICE_DISCOVERY_MODE));

                if (properties.getProperty(DBConnectionConstants.HIVE_ZOOKEEPER_NAMESPACE) == null || properties.getProperty(DBConnectionConstants.HIVE_ZOOKEEPER_NAMESPACE).isEmpty()) {
                    throw new InvalidDBPropertyException("zooKeeperNamespace cannot be empty");
                }
                jdbcConnectionStringBuffer.append(hiveDBProperties.getProperty(DBConnectionConstants.HIVE_JDBC_URL_PROPERTY_SEPARATOR));
                jdbcConnectionStringBuffer.append(DBConnectionConstants.HIVE_ZOOKEEPER_NAMESPACE + "=" + properties.getProperty(DBConnectionConstants.HIVE_ZOOKEEPER_NAMESPACE));

                logger.debug("JDBC string for zk : " + jdbcConnectionStringBuffer.toString());

            } else if (jdbcConnectWay.equalsIgnoreCase(DBConnectionConstants.HiveJDBCConnectWays.DIRECT.toString())) {
                if (properties.getProperty(DBConnectionConstants.HIVE_HOST) == null || properties.getProperty(DBConnectionConstants.HIVE_HOST).isEmpty()) {
                    throw new InvalidDBPropertyException("Hive host cannot be empty");
                }
                if (properties.getProperty(DBConnectionConstants.HIVE_PORT) == null || properties.getProperty(DBConnectionConstants.HIVE_PORT).isEmpty()) {
                    throw new InvalidDBPropertyException("Hive port cannot be empty");
                }
                if (properties.getProperty(DBConnectionConstants.HIVE_DB) == null || properties.getProperty(DBConnectionConstants.HIVE_DB).isEmpty()) {
                    throw new InvalidDBPropertyException("Hive db cannot be empty");
                }

                StringBuffer tempBuffer = new StringBuffer(properties.getProperty(DBConnectionConstants.HIVE_HOST));
                tempBuffer.append(properties.getProperty(DBConnectionConstants.HIVE_JDBC_URL_VALUE_SEPARATOR));
                tempBuffer.append(properties.getProperty(DBConnectionConstants.HIVE_PORT));
                tempBuffer.append(properties.getProperty(DBConnectionConstants.HIVE_JDBC_URL_DBNAME_SEPARATOR));
                tempBuffer.append(properties.getProperty(DBConnectionConstants.HIVE_DB));

                jdbcConnectionStringBuffer = new StringBuffer(String.format(hiveDBProperties.getProperty(DBConnectionConstants.HIVE_JDBC_URL), tempBuffer.toString()));

                logger.debug("JDBC String for direct : " + jdbcConnectionStringBuffer.toString());
            } else {
                throw new InvalidDBPropertyException("Provide the relevant properties for exactly one of the following Hive JDBC connection URL types " + System.lineSeparator()
                        + "1. Connection URL When ZooKeeper Service Discovery Is Enabled" + System.lineSeparator()
                        + "2. Directly specify the hive host:port/db" + System.lineSeparator()
                        + " Refer " + hiveDBProperties.getProperty(DBConnectionConstants.HIVE_WIKI_JDBC_WEBPAGE));
            }
            /*End : JDBC connection ways*/

            /*Start : Cluster security ways*/
            if (properties.getProperty(DBConnectionConstants.HIVE_CLUSTER_SECURITY_TYPE).equalsIgnoreCase(DBConnectionConstants.HiveClusterSecurityTypes.KERBERIZED.toString())
                    && jdbcConnectWay.equalsIgnoreCase(DBConnectionConstants.HiveJDBCConnectWays.DIRECT.toString())) {

            } else if (properties.getProperty(DBConnectionConstants.HIVE_CLUSTER_SECURITY_TYPE).equalsIgnoreCase(DBConnectionConstants.HiveClusterSecurityTypes.KERBERIZED_PREAUTH_SUBJECT.toString())) {
                if (properties.getProperty(DBConnectionConstants.HIVE_PRINCIPAL) == null || properties.getProperty(DBConnectionConstants.HIVE_PRINCIPAL).isEmpty()) {
                    throw new InvalidDBPropertyException("Principal cannot be empty while connecting to a kerberized cluster");
                }
                jdbcConnectionStringBuffer.append(hiveDBProperties.getProperty(DBConnectionConstants.HIVE_JDBC_URL_PROPERTY_SEPARATOR));
                jdbcConnectionStringBuffer.append(DBConnectionConstants.HIVE_PRINCIPAL + "=" + properties.getProperty(DBConnectionConstants.HIVE_PRINCIPAL));
                jdbcConnectionStringBuffer.append(hiveDBProperties.getProperty(DBConnectionConstants.HIVE_JDBC_URL_PROPERTY_SEPARATOR));
                jdbcConnectionStringBuffer.append(hiveDBProperties.getProperty(DBConnectionConstants.HIVE_PREAUTH_SUBJECT_KERBEROS_AUTH_TYPE) + "=" + hiveDBProperties.getProperty(DBConnectionConstants.HIVE_PREAUTH_SUBJECT_AUTH));

                logger.debug("JDBC string for kerberized + preauth : " + jdbcConnectionStringBuffer.toString());
            }
            /*End : Cluster security ways*/

            /*Start : Append the additional properties*/
            if (properties.getProperty(DBConnectionConstants.HIVE_HTTP_PATH) != null && !properties.getProperty(DBConnectionConstants.HIVE_HTTP_PATH).isEmpty()) {

                jdbcConnectionStringBuffer.append(DBConnectionConstants.HIVE_JDBC_URL_PROPERTY_SEPARATOR);
                jdbcConnectionStringBuffer.append(DBConnectionConstants.HIVE_HTTP_PATH + "=" + properties.getProperty(DBConnectionConstants.HIVE_HTTP_PATH));
            }
            if (properties.getProperty(DBConnectionConstants.HIVE_TRANSPORT_MODE) != null && !properties.getProperty(DBConnectionConstants.HIVE_TRANSPORT_MODE).isEmpty()) {

                jdbcConnectionStringBuffer.append(DBConnectionConstants.HIVE_JDBC_URL_PROPERTY_SEPARATOR);
                jdbcConnectionStringBuffer.append(DBConnectionConstants.HIVE_TRANSPORT_MODE + "=" + properties.getProperty(DBConnectionConstants.HIVE_TRANSPORT_MODE));
            }

            logger.debug("JDBC string after appending additional properties : " + jdbcConnectionStringBuffer.toString());
            /*End : Append the additional properties*/

            return connection;
        }
    }

    ;
}
