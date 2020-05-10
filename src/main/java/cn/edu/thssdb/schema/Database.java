package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.type.ColumnType;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

    private String name;
    private HashMap<String, Table> tables;
    ReentrantReadWriteLock lock;

    public Database(String name) {
        this.name = name;
        this.tables = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
        recover();
    }

    private void persist() {
        for (Table table: tables.values()) {
            File metaFile = new File("./" + this.name + "/" + table.tableName + ".meta");
            FileWriter writer;
            try {
                writer = new FileWriter(metaFile);
            } catch (Exception e) {
                throw new IOException("Cannot open table meta file of " + table.tableName);
            }
            for (Column column : table.columns) {
                try {
                    writer.write(column.toString() + "\n");
                } catch (Exception e) {
                    throw new IOException("Cannot write to table meta file of" + table.tableName);
                }
            }
        }
    }

    public void create(String name, Column[] columns) {
        try {
            lock.writeLock().lock();
            if (tables.containsKey(name)) {
                throw new DuplicateTableException(name);
            }
            boolean primary = false;
            HashSet<String> columnNames = new HashSet<>();
            // check column validity
            for (Column column : columns) {
                String columnName = column.getName();
                if (columnName.equals("uuid")) {
                    throw new FieldNameException();
                }
                if (columnNames.contains(columnName)) {
                    throw new DuplicateColumnException(columnName);
                } else {
                    columnNames.add(columnName);
                    if (column.getPrimary() == 1) {
                        if (primary) {
                            throw new DuplicatePrimaryException(columnName);
                        } else {
                            primary = true;
                        }
                    }
                }
            }

            Table table;
            if (primary) {
                table = new Table(this.name, name, columns);
            } else {
                Column primaryColumn = new Column("uuid", ColumnType.LONG, 1, true, -1);
                Column[] newColumns = new Column[columns.length + 1];
                System.arraycopy(columns, 0, newColumns, 0, columns.length);
                newColumns[columns.length] = primaryColumn;
                table = new Table(this.name, name, newColumns);
            }

            tables.put(name, table);

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void dropAll() {
        for (String name : tables.keySet()) {
            Table table = tables.get(name);
            table.deleteAll();
            File file = new File("./" + this.name + "/" + name + ".meta");
            file.delete();
        }
        tables.clear();
    }

    public void drop(String name) {
        Table table = tables.get(name);
        if (table == null) {
            throw new KeyNotExistException();
        }
        table.deleteAll();
        tables.remove(name);
        File file = new File("./" + this.name + "/" + name + ".meta");
        file.delete();
    }

    public String select(QueryTable[] queryTables) {
        try {
            lock.readLock().lock();
            QueryResult queryResult = new QueryResult(queryTables);
            for (QueryTable queryTable : queryTables) {
                // TODO
            }
        } finally {
            lock.readLock().unlock();
        }
        return null;
    }

    private void recover() {
        File dir = new File("./" + this.name);
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            String fileName = file.getName();
            String[] names = fileName.split(".");
            if (names.length != 2 || !names[1].equals("meta")) {
                continue;
            }

            BufferedReader reader;
            try {
                reader = new BufferedReader(new FileReader(file));
            } catch (Exception e) {
                throw new IOException("Cannot open table meta file of " + names[0]);
            }
            String line;
            ArrayList<Column> columns = new ArrayList<>();
            while (true) {
                try {
                    if ((line = reader.readLine()) == null) {
                        break;
                    }
                } catch (Exception e) {
                    throw new IOException("Cannot read table meta file of " + names[0]);
                }
                String[] columnInfo = line.split(",");
                if (columnInfo.length != 5) {
                    throw new IOException("Table meta file of " + names[0] + " is corrupted");
                }
                // name, type, primary, notNull, maxLength
                // TODO: Might need to handle type error here
                String name = columnInfo[0];
                ColumnType type = ColumnType.valueOf(columnInfo[1]);
                int primary = Integer.parseInt(columnInfo[2]);
                boolean notNull = Boolean.parseBoolean(columnInfo[3]);
                int maxLength = Integer.parseInt(columnInfo[4]);
                Column column = new Column(name, type, primary, notNull, maxLength);
                columns.add(column);
            }
            try {
                reader.close();
            } catch (Exception e) {
                throw new IOException("Cannot close table meta file of " + names[0]);
            }

            Table table = new Table(this.name, names[0], columns.toArray(new Column[0]));
            this.tables.put(names[0], table);
        }
    }

    public void quit() {
        try {
            lock.writeLock().lock();
            persist();
            for (Table table : tables.values()) {
                table.commit();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
