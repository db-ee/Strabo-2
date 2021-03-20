package it.unibz.krdb.obda.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * @author dimitris bilidas
 * @author herald kllapi
 */

public class StrabonProperties {
    private static final Logger log = LoggerFactory.getLogger(StrabonProperties.class);
    private static final GenericProperties strabonProperties;
    private static String NEW_LINE = System.getProperty("line.separator");

    static {

        try {

            strabonProperties = PropertiesFactory.loadMutableProperties("strabon");
            // load
            Properties properties = System.getProperties();
            for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
                properties.setProperty(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            log.error("Cannot initialize properties", e);
            throw new RuntimeException("can not init props!");
        }
    }

    public StrabonProperties() {
        throw new RuntimeException("Cannot create instance of this class");
    }

    public static String getNewLine() {
        return NEW_LINE;
    }

    public static GenericProperties getStrabonProperties() {
        return strabonProperties;
    }
}
