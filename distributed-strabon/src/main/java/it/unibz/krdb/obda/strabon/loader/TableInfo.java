package it.unibz.krdb.obda.strabon.loader;

public class TableInfo {

    private long countAll;
    private long distinctSubjects;
    private long distinctObjects;

    public TableInfo(long rows, long rows2, long rows3) {
        super();
        this.countAll = rows;
        this.distinctSubjects = rows2;
        this.distinctObjects = rows3;
    }

    public long getCountAll() {
        return countAll;
    }

    public void setCountAll(int countAll) {
        this.countAll = countAll;
    }

    public long getDistinctSubjects() {
        return distinctSubjects;
    }

    public void setDistinctSubjects(int distinctSubjects) {
        this.distinctSubjects = distinctSubjects;
    }

    public long getDistinctObjects() {
        return distinctObjects;
    }

    public void setDistinctObjects(int distinctObjects) {
        this.distinctObjects = distinctObjects;
    }

}
