package it.unibz.krdb.obda.strabon.configuration;

import java.io.Serializable;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class PredTbl implements Serializable {

    // ----- STATIC MEMBERS -----
    public static final Encoder<PredTbl> personEncoder = Encoders.bean(PredTbl.class);

    // ----- DATA MEMBERS -----
    private String pred; // IRI for (p)redicate in RDF triple (s, p, o)
    private String tblName; // Hive table name
    private long records;
    private long distSubjects;
    private long distObjects;

    // ----- CONSTRUCTORS -----
    public PredTbl(String pred, String tbl, long records, long distSubjects, long distObjects) {
        this.pred = pred;
        this.tblName = tbl;
        this.records = records;
        this.distSubjects = distSubjects;
        this.distObjects = distObjects;
    }

    // ----- DATA ACCESSORS -----
    public String getPred() {
        return pred;
    }

    public void setPred(String pred) {
        this.pred = pred;
    }

    public String getTblName() {
        return tblName;
    }

    public void setTblName(String tblName) {
        this.tblName = tblName;
    }

    public long getRecords() {
        return records;
    }

    public void setRecords(long records) {
        this.records = records;
    }

    public long getDistSubjects() {
        return distSubjects;
    }

    public void setDistSubjects(long distSubjects) {
        this.distSubjects = distSubjects;
    }

    public long getDistObjects() {
        return distObjects;
    }

    public void setDistObjects(long distObjects) {
        this.distObjects = distObjects;
    }

    // ----- METHODS --------
    @Override
    public String toString() {
        return "(" + this.pred + " , " + this.tblName + " , " + this.records + " , " + this.distSubjects + " , " + this.distObjects + ")";
    }
}
