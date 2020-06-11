package cn.edu.thssdb.utils;

import java.util.concurrent.locks.ReentrantLock;

public class Context {
    public String databaseName;

    public ReentrantLock mutex = new ReentrantLock(true);

    public boolean autoCommit = true;

    public Context(String dbName) { databaseName = dbName; }
}
