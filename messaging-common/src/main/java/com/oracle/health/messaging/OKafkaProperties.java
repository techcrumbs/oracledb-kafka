package com.oracle.health.messaging;


import java.io.File;
import java.util.Objects;
import java.util.Properties;

public class OKafkaProperties {

    public static Properties getLocalConnectionProps(String ojdbcPropertiesFile, Integer port)  {
        String directoryPath = new File(ojdbcPropertiesFile).getAbsolutePath();
        System.out.println("ojdcProperties directory = " + directoryPath);
        Properties props = new Properties();
        // We use the default PDB for Oracle Database 23ai.
        props.put("oracle.service.name", "freepdb1");
        // The localhost connection uses PLAINTEXT.
        props.put("security.protocol", "PLAINTEXT");
        props.put("bootstrap.servers", String.format("localhost:%d",
                Objects.requireNonNullElse(port, 1521)));
        props.put("oracle.net.tns_admin", directoryPath);
        return props;
    }

}
