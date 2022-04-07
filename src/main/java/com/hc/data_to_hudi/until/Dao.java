package com.hc.data_to_hudi.until;

public class Dao {
    private String dbName;
    private String tbName;
    private String data;

    public String getTbName() {
        return tbName;
    }

    public void setTbName(String tbName) {
        this.tbName = tbName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public Dao() {
    }

    public Dao(String dbName, String tbName, String data) {
        this.dbName = dbName;
        this.tbName = tbName;
        this.data = data;
    }

    @Override
    public String toString() {
        return "Dao{" +
                "dbName='" + dbName + '\'' +
                ", tbName='" + tbName + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
