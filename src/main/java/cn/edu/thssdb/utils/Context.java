package cn.edu.thssdb.utils;

import java.util.HashMap;

public class Context {
    public String databaseName;

    public boolean autoCommit = true;

    public Context(String dbName) { databaseName = dbName; }
}
