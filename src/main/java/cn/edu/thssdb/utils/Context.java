package cn.edu.thssdb.utils;

public class Context {
    public String databaseName;

    public boolean autoCommit = true;

    public Context(String dbName) { databaseName = dbName; }
}
