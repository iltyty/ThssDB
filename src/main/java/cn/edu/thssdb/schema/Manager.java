package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateDatabaseException;
import cn.edu.thssdb.exception.IOException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.utils.Context;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
    private HashMap<String, Database> databases;
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public static Manager getInstance() {
        return Manager.ManagerHolder.INSTANCE;
    }

    public Manager() {
        this.databases = new HashMap<>();
        recoverDatabase();
        createDatabaseIfNotExists();
    }

    private void recoverDatabase() {
        File file = new File("./db.meta");
        if (!file.exists()) {
            return;
        }
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(file));
        } catch (Exception e) {
            throw new IOException("Error reading manager meta file");
        }
        reader.lines().forEach(name -> {
            Database db = new Database(name);
            databases.put(name, db);
        });
        try {
            reader.close();
        } catch (Exception e) {
            throw new IOException("Error closing manager meta file");
        }
    }

    private void createDatabaseIfNotExists() {
        try {
            lock.writeLock().lock();
            if (databases.containsKey("admin")) {
                return;
            }
            createDatabase("admin");
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void deleteDatabase(String name) {
        try {
            lock.writeLock().lock();
            Database db = databases.get(name);
            if (db == null) {
                throw new KeyNotExistException();
            }
            db.dropAll();
            databases.remove(name);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void switchDatabase(String name, Context context) {
        try {
            lock.readLock().lock();
            if (!databases.containsKey(name)) {
                throw new KeyNotExistException();
            }
            context.databaseName = name;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void createDatabase(String name) {
        try {
            lock.writeLock().lock();
            if (databases.containsKey(name)) {
                throw new DuplicateDatabaseException(name);
            }
            Database db = new Database(name);
            // create directory for database
            File dir = new File("./" + name);
            if (!dir.exists()) {
                dir.mkdir();
            }
            databases.put(name, db);
        } finally {
            lock.writeLock().unlock();
        }

    }

    private static class ManagerHolder {
        private static final Manager INSTANCE = new Manager();

        private ManagerHolder() {

        }
    }
}
