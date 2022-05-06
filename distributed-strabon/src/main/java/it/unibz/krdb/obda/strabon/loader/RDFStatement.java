package it.unibz.krdb.obda.strabon.loader;

import java.io.Serializable;

import it.unibz.krdb.obda.strabon.DataLoader;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class RDFStatement implements Serializable {

    // ----- STATIC MEMBERS -----
    protected static final Logger logger = Logger.getLogger(DataLoader.appName);
    private static final long serialVersionUID = 42L;
    // Encoder is created for Java bean Namespace
    public static final Encoder<RDFStatement> Encoder = Encoders.bean(RDFStatement.class);

    private String o;
    private String s;
    private String p;
    private int oType;

    public RDFStatement(String subj, String pred, String obj, int objType) {
        this.o = obj;
        this.p = pred;
        this.s = subj;
        this.oType = objType;
    }

    public RDFStatement() {
    }

    public String getO() {
        return o;
    }

    public void setO(String o) {
        this.o = o;
    }

    public String getS() {
        return s;
    }

    public void setS(String s) {
        this.s = s;
    }

    public String getP() {
        return p;
    }

    public void setP(String p) {
        this.p = p;
    }

    public int getoType() {
        return oType;
    }

    public void setoType(int oType) {
        this.oType = oType;
    }

    public String serialize() {
        return this.s + "\t"
                + this.p + "\t"
                + this.o;
    }

    public static RDFStatement deserialize(String serializedString, int oType) {
        String[] elements = serializedString.split("\t");
        if (elements.length == 2) { // usually an "" object
            String[] newelem = new String[3];
            newelem[0] = elements[0];
            newelem[1] = elements[1];
            newelem[2] = "";
            elements = newelem;
        }
        return new RDFStatement(elements[0], elements[1], elements[2], oType);

    }
}
