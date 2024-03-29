package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.query.Where;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {
    private static final Logger logger = LoggerFactory.getLogger(Database.class);

    private Context context;

    private String name;
    public HashMap<String, Table> tables;
    ReentrantReadWriteLock lock;

    public Database(String name, Context context) {
        this.name = name;
        this.tables = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.context = context;
        recover();
    }

    private void persist() {
        for (Table table: tables.values()) {
            persistTable(table);
        }
    }

    private void persistTable(Table table) {
        File metaFile = new File("./" + name + "/" + table.tableName + ".meta");
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
                throw new IOException("Cannot write to table meta file of " + table.tableName);
            }
        }
        try {
            writer.close();
        } catch (Exception e) {
            throw new IOException("Cannot close meta file descriptor of " + table.tableName);
        }
    }

    public void create(String name, Column[] columns) {
        try {
            lock.writeLock().lock();
            if (tables.containsKey(name)) {
                throw new DuplicateTableException(name);
            }

            // whether there is already a single primary key
            int primaryExisted = 0;
            HashSet<String> columnNames = new HashSet<>();
            // check column validity
            for (Column column : columns) {
                String columnName = column.getName();
                if (columnName.equals("uuid")) {
                    throw new FieldNameException();
                }
                if (columnNames.contains(columnName)) {
                    throw new DuplicateColumnException(columnName);
                }

                columnNames.add(columnName);
                if (column.getPrimary() == 1) {
                    if (primaryExisted != 0) {
                        throw new DuplicatePrimaryException(columnName);
                    }
                    primaryExisted = 1;
                } else if (column.getPrimary() == 2) {
                    if (primaryExisted == 1) {
                        throw  new DuplicateDatabaseException(columnName);
                    }
                    primaryExisted = 2;
                }
            }

            Table table;
            if (primaryExisted == 1) {
                // single primary key
                table = new Table(this.name, name, columns, context);
            } else {
                // composite primary key
                Column primaryColumn = new Column("uuid", ColumnType.LONG, 1, true, -1);
                Column[] newColumns = new Column[columns.length + 1];
                System.arraycopy(columns, 0, newColumns, 0, columns.length);
                newColumns[columns.length] = primaryColumn;
                table = new Table(this.name, name, newColumns, context);
            }

            tables.put(name, table);

            if (context.autoCommit) {
                persistTable(table);
            }
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

    public QueryResult select(QueryTable[] queryTables, String[] columnNames, Where where, boolean distinct) {
        try {
            lock.readLock().lock();
            logger.info("Getting full query result");
            return getFullQueryResult(queryTables, columnNames, where, distinct);
        } finally {
            lock.readLock().unlock();
        }
    }

    private QueryResult getFullQueryResult(QueryTable[] queryTables, String[] columnNames, Where where, boolean distinct) {
        // cartesian product
        QueryResult queryResult = new QueryResult(queryTables, columnNames, where, distinct);
        LinkedList<Row> rows = new LinkedList<>();
        while (true) {
            if (rows.isEmpty()) {
                for (QueryTable queryTable : queryTables) {
                    if (!queryTable.hasNext()) {
                        return queryResult;
                    }
                    rows.push(queryTable.next());
                }
            } else {
                int index;
                for (index = queryTables.length - 1; index >= 0; index--) {
                    rows.pop();
                    if (!queryTables[index].hasNext()) {
                        queryTables[index].clear();
                    } else {
                        break;
                    }
                }
                if (index < 0) {
                    break;
                }
                for (int i = index; i < queryTables.length; i++) {
                    if (!queryTables[i].hasNext()) {
                        break;
                    }
                    rows.push(queryTables[i].next());
                }
            }
            if (rows.stream().anyMatch(Objects::isNull)) {
                return queryResult;
            }
            queryResult.addRow(rows);
        }
        return queryResult;
    }

    private void recover() {
        File dir = new File("./" + this.name);
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        File logFile = new File("./" + this.name + "/.log");
        if (logFile.exists()) {
            logFile.delete();
        }
        for (File file : files) {
            String fileName = file.getName();
            String[] names = fileName.split("\\.");
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

            Table table = new Table(this.name, names[0], columns.toArray(new Column[0]), context);
            this.tables.put(names[0], table);
        }
    }

    public Table getTable(String name) {
        try {
            lock.readLock().lock();
            if (!tables.containsKey(name)) {
                throw new KeyNotExistException();
            }
            return tables.get(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void commit() {
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
