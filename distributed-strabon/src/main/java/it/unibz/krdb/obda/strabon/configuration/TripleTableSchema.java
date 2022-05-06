package it.unibz.krdb.obda.strabon.configuration;

/**
 *
 * @author tioannid
 */
public class TripleTableSchema implements java.io.Serializable {

    // DATA MEMBERS
    private String tblName;
    private String subjColName;
    private String predColName;
    private String objColName;
    private String objTypeColName;
    private String tblFormat;

    // CONSTRUCTORS
    // 1. Detailed/Base Constructor
    public TripleTableSchema(String tblname, String colname_subj,
            String colname_pred, String colname_obj,
            String colname_objtype, String tblFormat) {
        this.tblName = tblname;
        this.subjColName = colname_subj;
        this.predColName = colname_pred;
        this.objColName = colname_obj;
        this.objTypeColName = colname_objtype;
        this.tblFormat = tblFormat;
    }

    // 2. Constructor with some default values
    public TripleTableSchema() {
        this("triples", "s", "p", "o", "OType", "parquet");
    }

    // DATA ACCESSORS
    public String getTblname() {
        return tblName;
    }

    public void setTblname(String tblname) {
        this.tblName = tblname;
    }

    public String getColname_subj() {
        return subjColName;
    }

    public void setColname_subj(String colname_subj) {
        this.subjColName = colname_subj;
    }

    public String getColname_pred() {
        return predColName;
    }

    public void setColname_pred(String colname_pred) {
        this.predColName = colname_pred;
    }

    public String getColname_obj() {
        return objColName;
    }

    public void setColname_obj(String colname_obj) {
        this.objColName = colname_obj;
    }

    public String getColname_objtype() {
        return objTypeColName;
    }

    public void setColname_objtype(String colname_objtype) {
        this.objTypeColName = colname_objtype;
    }

    public String getTblformat() {
        return tblFormat;
    }

    public void setTblformat(String tblformat) {
        this.tblFormat = tblformat;
    }
}
