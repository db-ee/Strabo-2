package sesameWrapper;

import it.unibz.krdb.obda.utils.GenericProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * @author dimitris bilidas
 * @author herald kllapi
 */

public class ConnectionProperties {
    private static final Logger log = LoggerFactory.getLogger(ConnectionProperties.class);
    private static final GenericProperties connectionProperties;
    private static String NEW_LINE = System.getProperty("line.separator");

    static {

        try {
            Properties properties = new Properties();
            File jar = new File(ConnectionProperties.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            FileInputStream propFIle =new FileInputStream(jar.getParent().toString()+"/../connection.properties");
            //InputStream in = ServletContext.getResourceAsStream("/WEB-INF/connection.properties");
            properties.load(propFIle);
            connectionProperties = new GenericProperties(properties);


        } catch (Exception e) {
            log.error("Cannot initialize properties", e);
            throw new RuntimeException("can not init props!");
        }
    }

    public ConnectionProperties() {
        throw new RuntimeException("Cannot create instance of this class");
    }

    public static String getNewLine() {
        return NEW_LINE;
    }

    public static GenericProperties getConnectionProperties() {
        return connectionProperties;
    }
}
