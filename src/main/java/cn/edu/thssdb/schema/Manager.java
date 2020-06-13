package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateDatabaseException;
import cn.edu.thssdb.exception.IOException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.RelationNotExist;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.transaction.LockManager;
import cn.edu.thssdb.utils.Context;
import cn.edu.thssdb.utils.Global;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Manager {
    private HashMap<String, Database> databases;
    public Context context;
    public LockManager lockManager;
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public static Manager getInstance() {
        return Manager.ManagerHolder.INSTANCE;
    }

    public Manager(Context cxt) {
        this.context = cxt;
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
            Database db = new Database(name, context);
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
            if (databases.containsKey(Global.ADMIN_DB_NAME)) {
                return;
            }
            createDatabase(Global.ADMIN_DB_NAME);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void syncMetafile() {
        File metafile = new File("./db.meta");
        try {
            metafile.createNewFile();
            FileWriter writer = new FileWriter(metafile);
            for (String dbName : databases.keySet()) {
                writer.write(dbName + "\n");
            }
            writer.close();
        } catch (java.io.IOException e) {
            throw new IOException("Error writing to manager meta file");
        }
    }

    public void deleteDatabase(String name) {
        try {
            lock.writeLock().lock();
            Database db = databases.get(name);
            if (db == null) {
                throw new KeyNotExistException();
            }
            db.dropAll();
            databases.remove(name);
            syncMetafile();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void switchDatabase(String name) {
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

    public void createDatabase(String name) {
        try {
            lock.writeLock().lock();
            if (databases.containsKey(name)) {
                throw new DuplicateDatabaseException(name);
            }
            Database db = new Database(name, context);
            // create directory for database
            File dir = new File("./" + name);
            if (!dir.exists()) {
                dir.mkdir();
            }
            databases.put(name, db);
            // write database names to metafile
            syncMetafile();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Database getDatabase(String name) {
        try {
            lock.readLock().lock();
            if (!databases.containsKey(name)) {
                throw new KeyNotExistException();
            }
            return databases.get(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    public String showDatabases() {
        StringJoiner sj = new StringJoiner(" ");
        try {
            lock.readLock().lock();
            for (String name : databases.keySet()) {
                sj.add(name);
            }
        } finally {
            lock.readLock().unlock();
        }
        return "All databases: " + sj.toString();
    }

    public String showTables(String name) {
        Database db = getDatabase(name);
        StringJoiner sj = new StringJoiner(" ");
        try {
            db.lock.readLock().lock();
            for (String s : db.tables.keySet()) {
                sj.add(s);
            }
        } finally {
            db.lock.readLock().unlock();
        }
        return String.format("Tabels in %s: %s", name, sj.toString());
    }

    public void insert(String tableName, String[] values, String[] columnNames) {
        Database database = getDatabase(context.databaseName);
        Table table = database.getTable(tableName);
        table.insert(values, columnNames);
    }

    public void createTable(String name, Column[] columns) {
        Database database = getDatabase(context.databaseName);
        database.create(name, columns);
    }

    public void deleteTable(String name, boolean optional) {
        Database db = getDatabase(context.databaseName);
        try {
            db.lock.writeLock().lock();
            if (!db.tables.containsKey(name)) {
                if (optional) {
                    return;
                }
                throw new KeyNotExistException();
            }
            db.drop(name);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public QueryResult select(String[] columnNames, QueryTable[] queryTables, Where where, boolean distinct) {
        Database database = getDatabase(context.databaseName);
        return database.select(queryTables, columnNames, where, distinct);
    }

    public QueryTable getSingleTable(String tableName) {
        Database database = getDatabase(context.databaseName);
        try {
            database.lock.readLock().lock();
            if (database.tables.containsKey(tableName)) {
                return new SingleTable(database.tables.get(tableName));
            }
        } finally {
            database.lock.readLock().unlock();
        }
        throw new RelationNotExist(tableName);
    }

    public QueryTable getJointTable(List<String> tableNames, Where join) {
        Database database = getDatabase(context.databaseName);
        try {
            database.lock.readLock().lock();
            for (int i = 0; i < tableNames.size(); i++) {
                if (!database.tables.containsKey(tableNames.get(i))) {
                    throw new RelationNotExist(tableNames.get(i));
                }
            }
            List<Table> tables = tableNames.stream()
                    .map(name -> database.tables.get(name))
                    .collect(Collectors.toList());
            return new JointTable(tables, join);
        } finally {
            database.lock.readLock().unlock();
        }
    }

    public int update(String tableName, String columnName, Expr value, Where where) {
        Database database = getDatabase(context.databaseName);
        try {
            database.lock.writeLock().lock();
            Table table = database.tables.get(tableName);
            return table.update(columnName, value, where);
        } finally {
            database.lock.writeLock().unlock();
        }
    }

    public int delete(String tableName, Where where) {
        Database database = getDatabase(context.databaseName);
        try {
            database.lock.writeLock().lock();
            Table table = database.tables.get(tableName);
            return table.delete(where);
        } finally {
            database.lock.writeLock().unlock();
        }
    }

    public void beginTransaction() {
        context.mutex.lock();
        context.autoCommit = false;
    }

    public void endTransaction() {
        context.autoCommit = true;
        context.mutex.unlock();
    }

    public void commit() {
        for (Database database : databases.values()) {
            database.commit();
        }
    }

    private static class ManagerHolder {
        private static final Manager INSTANCE = new Manager(new Context(Global.ADMIN_DB_NAME));

        private ManagerHolder() {

        }
    }
}
