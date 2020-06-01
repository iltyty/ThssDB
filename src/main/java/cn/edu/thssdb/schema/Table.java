package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.IOException;
import cn.edu.thssdb.exception.ValueException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.query.Expr;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.Where;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Context;
import cn.edu.thssdb.utils.Global;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;

public class Table implements Iterable<Row> {
    private Context context;
    ReentrantReadWriteLock lock;
    private String databaseName;
    public String tableName;
    public ArrayList<Column> columns;
    public BPlusTree<Entry, Row> index;
    private int primaryIndex;
    private HashMap<Integer, Page> pages;
    private int currentPage;

    public Table(String databaseName, String tableName, Column[] columns, Context ctx) {
        this.context = ctx;
        this.lock = new ReentrantReadWriteLock();
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<>(Arrays.asList(columns));
        this.index = new BPlusTree<>();
        this.pages = new HashMap<>();

        for (int i = 0; i < columns.length; i++) {
            if (this.columns.get(i).getPrimary() == 1) {
                // is primary key
                primaryIndex = i;
            }
        }

        currentPage = -1;
        recover();
        if (currentPage == -1) {
            getNewPage();
        }
    }

    private void recover() {
        String dbPath = "./" + this.databaseName + "/";
        File dbDir = new File(dbPath);
        File[] files = dbDir.listFiles();
        if (files == null) {
            throw new IOException("Database path does not exist");
        } else if (files.length == 0) {
            return;
        }
        for (File file : files) {
            String[] filename = file.getName().split("\\.");
            if (filename.length != 3 || !filename[0].equals(this.tableName) || !filename[2].equals("dat")) {
                continue;
            }
            int id;
            try {
                id = Integer.parseInt(filename[1]);
            } catch (Exception e) {
                continue;
            }
            currentPage = id;

            Page page = new Page(databaseName, tableName, id);
            ArrayList<Row> rows;
            try {
                rows = deserialize(file);
            } catch (Exception e) {
                throw new IOException("Error deserialize into file");
            }
            for (Row row : rows) {
                ArrayList<Entry> entries = row.getEntries();
                for (int i = 0; i < entries.size(); i++) {
                    if (i == primaryIndex) {
                        try {
                            index.put(entries.get(i), row);
                        } catch (DuplicateKeyException ignored) {
                        }
                        page.entries.add(entries.get(i));
                        break;
                    }
                }
            }

            pages.put(id, page);
        }
    }

    private boolean hasColumn(String columnName) {
        for (Column column : columns) {
            if (column.getName().equals(columnName)) {
                return true;
            }
        }

        return false;
    }

    public MetaInfo getMetaInfo() {
        return new MetaInfo(tableName, columns);
    }

    public void insert(String[] values, String[] columnNames) {
        if (columnNames != null) {
            if (values == null || values.length != columns.size() || values.length != columnNames.length) {
                throw new ValueException("Count of values does not match count of columns");
            }
            for (String name : columnNames) {
                if (!hasColumn(name)) {
                    throw new ColumnNotExistException(name);
                }
            }
        }
        Entry[] entries = new Entry[columns.size()];
        Entry primaryEntry = null;
        for (int i = 0; i < values.length; i++) {
            Entry entry = new Entry(0);
            Column column = columns.get(i);
            if (columnNames == null) {
                entry = getValue(values[i], column.getType());
            } else {
                boolean found = false;
                for (int j = 0; j < values.length; j++) {
                    if (columnNames[j].equals(column.getName())) {
                        entry = getValue(values[j], column.getType());
                        found = true;
                        break;
                    }
                }
                if (!found && column.notNull) {
                    throw new ValueException("No value specified for column " + column.getName());
                }
            }
            if (column.getType() == ColumnType.STRING && entry.value != null && String.valueOf(entry.value).length() > column.maxLength) {
                throw new ValueException("String of column " + column.getName() + " exceeds maxLength");
            }
            entries[i] = entry;
            if (i == primaryIndex) {
                primaryEntry = entry;
            }
        }
        assert primaryEntry != null;
        try {
            lock.writeLock().lock();
            Row row = new Row(entries, currentPage);
            Page page = pages.get(currentPage);
            assert page != null;
            page.insert(primaryEntry, row.toString().length());
            page.dirty = true;
            if (page.size > Global.PAGE_SIZE) {
                getNewPage();
            }
            index.put(primaryEntry, row);

            if (context.autoCommit) {
                commit();
            }
        } finally {
            lock.writeLock().unlock();
        }

    }

    public int delete(Where where) {
        // delete rows that matches a predicate?
        // For now, I will just use a `Predicate<Row>`
        Predicate<Row> predicate = null;
        if (where != null) {
            predicate = where.toPredicate(new ArrayList<MetaInfo>() {{
                add(getMetaInfo());
            }});
        }
        try {
            lock.writeLock().lock();
            int deleteCount = 0;
            for (Row row : this) {
                if (predicate == null || predicate.test(row)) {
                    deleteCount++;
                    Entry primaryKey = row.getEntries().get(primaryIndex);
                    index.remove(primaryKey);
                    Page page = pages.get(row.getPage());
                    page.delete(primaryKey, row.toString().length());
                    page.dirty = true;
                }
            }

            if (context.autoCommit) {
                commit();
            }

            return deleteCount;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void deleteAll() {
        for (int i = 0; i <= currentPage; i++) {
            File file = new File("./" + databaseName + "/" + tableName + "." + i + ".dat");
            file.delete();
        }
    }

    public int update(String columnName, Expr expr, Where where) {
        Column column = null;
        int i;
        for (i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                column = columns.get(i);
                break;
            }
        }
        if (column == null) {
            throw new ColumnNotExistException(columnName);
        }
        MetaInfo metaInfo = getMetaInfo();
        List<MetaInfo> metaInfos = new ArrayList<>();
        metaInfos.add(metaInfo);
        Predicate<Row> predicate = null;
        if (where != null) {
            predicate = where.toPredicate(metaInfos);
        }
        Function<Row, Comparable> func = expr.extractor(metaInfos);

        try {
            lock.writeLock().lock();
            int updateCount = 0;
            for (Row row : this) {
                if (predicate == null || predicate.test(row)) {
                    updateCount++;
                    int oldSize = row.toString().length();
                    Page page = pages.get(row.getPage());
                    Entry oldEntry = row.getEntries().get(primaryIndex);
                    Comparable newValue = func.apply(row);
                    newValue = Global.comparableToColumnType(newValue, column);
                    Entry newEntry = new Entry(newValue);
                    if (column.getPrimary() == 1) {
                        row.entries.set(i, newEntry);
                        page.entries.remove(oldEntry);
                        page.entries.add(newEntry);
                        if (index.contains(newEntry)) {
                            index.remove(oldEntry);
                            index.update(newEntry, row);
                        } else {
                            index.remove(oldEntry);
                            index.put(newEntry, row);
                        }
                    } else {
                        row.entries.set(i, newEntry);
                    }
                    page.dirty = true;
                    int newSize = row.toString().length();
                    page.updateSize(oldSize, newSize);
                }
            }

            if (context.autoCommit) {
                commit();
            }

            return updateCount;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void commit() {
        try {
            lock.writeLock().lock();
            for (Page page : pages.values()) {
                if (page.dirty) {
                    try {
                        serialize(page);
                    } catch (java.io.IOException e) {
                        throw new IOException("Cannot serialize into file");
                    }
                    page.dirty = false;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void serialize(Page page) throws java.io.IOException {
        File file = new File(page.path);
        ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(file));
        ArrayList<Row> rows = new ArrayList<>();
        for (Entry entry : page.entries) {
            Row row = index.get(entry);
            rows.add(row);
        }
        stream.writeObject(rows);
        stream.close();
    }

    private ArrayList<Row> deserialize(File file) throws java.io.IOException, ClassNotFoundException {
        ObjectInputStream stream = new ObjectInputStream(new FileInputStream(file));
        ArrayList<Row> rows = (ArrayList<Row>) stream.readObject();
        stream.close();
        return rows;
    }

    private void getNewPage() {
        currentPage++;
        Page page = new Page(databaseName, tableName, currentPage);
        pages.put(currentPage, page);
    }

    private Entry getValue(String value, ColumnType type) {
        Comparable comp;
        try {
            switch (type) {
                case INT:
                    comp = Integer.parseInt(value);
                    break;
                case LONG:
                    comp = Long.parseLong(value);
                    break;
                case FLOAT:
                    comp = Float.parseFloat(value);
                    break;
                case DOUBLE:
                    comp = Double.parseDouble(value);
                    break;
                case STRING:
                    comp = value;
                    break;
                default:
                    throw new ValueException("Unexpected column type");
            }
        } catch (Exception e) {
            throw new ValueException(e.getMessage());
        }

        return new Entry(comp);
    }

    private class TableIterator implements Iterator<Row> {
        private Iterator<Pair<Entry, Row>> iterator;

        TableIterator(Table table) {
            this.iterator = table.index.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Row next() {
            return iterator.next().getValue();
        }
    }

    @Override
    public Iterator<Row> iterator() {
        return new TableIterator(this);
    }
}
