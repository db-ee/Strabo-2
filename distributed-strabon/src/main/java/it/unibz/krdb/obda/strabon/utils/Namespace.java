package it.unibz.krdb.obda.strabon.utils;

import java.io.Serializable;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

/**
 *
 * @author tioannid
 */
public class Namespace implements Serializable {

    // ----- STATIC MEMBERS -----
    // Encoder is created for Java bean Namespace
    public static final Encoder<Namespace> Encoder = Encoders.bean(Namespace.class);
    public static final String NamespacePostDelimiter = ":";

    // ----- DATA MEMEBERS -----
    private String namespace;
    private String uri; // URI without < >

    // ----- CONSTRUCTORS -----
    public Namespace() {    
    }

    // ----- DATA ACCESSORS -----
    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace.concat(NamespacePostDelimiter);
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }
    
    // ----- METHODS -----
    public String getTurtlePrefixLine() {
        return String.format("PREFIX %1$s <%2$s>\n", namespace, uri);
    }
}
