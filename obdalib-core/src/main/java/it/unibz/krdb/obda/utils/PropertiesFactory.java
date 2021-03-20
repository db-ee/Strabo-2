package it.unibz.krdb.obda.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Enumeration;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * @author herald
 * @author alex
 */
public class PropertiesFactory {

    private static final Logger log = LoggerFactory.getLogger(PropertiesFactory.class);

    public static GenericProperties loadProperties(String propertiesName) throws Exception {
        Properties properties = new Properties();
        try {
            log.debug("Loading default properties (" + propertiesName + ") ...");
            ResourceBundle rb = ResourceBundle.getBundle(propertiesName);
            Enumeration<String> keys = rb.getKeys();
            while (keys.hasMoreElements()) {
                String key = keys.nextElement();
                properties.setProperty(key, rb.getObject(key).toString());
            }
        } catch (Exception e) {
            log.warn("Unable to load  properties (" + propertiesName + ").");
        }

        return new GenericProperties(properties);
    }

    public static MutableProperties loadMutableProperties(String propertiesName) throws Exception {

        Properties properties = new Properties();
        try {
            log.debug("Loading default properties (" + propertiesName + ") ...");
            ResourceBundle rb = ResourceBundle.getBundle(propertiesName);
            Enumeration<String> keys = rb.getKeys();
            while (keys.hasMoreElements()) {
                String key = keys.nextElement();
                properties.setProperty(key, rb.getObject(key).toString());
            }

        } catch (Exception e) {
            log.warn("Unable to load  properties (" + propertiesName + ").");
        }
        return new MutableProperties(properties);
    }
}
