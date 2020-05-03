package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.IOException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Global;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

public class Table implements Iterable<Row> {
    ReentrantReadWriteLock lock;
    private String databaseName;
    public String tableName;
    public ArrayList<Column> columns;
    public BPlusTree<Entry, Row> index;
    private int primaryIndex;

    public Table(String databaseName, String tableName, Column[] columns) {
        this.lock = new ReentrantReadWriteLock();
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<>(Arrays.asList(columns));
        this.index = new BPlusTree<>();

        for (int i = 0; i < columns.length; i++) {
            if (this.columns.get(i).getPrimary() == 1) {
                // is primary key
                primaryIndex = i;
            }
        }
    }

    private void recover() {
        String dbPath = "./" + this.databaseName + "/";
        File dbDir = new File(dbPath);
        File[] files = dbDir.listFiles();
        if (files == null) {
            throw new IOException();
        } else if (files.length == 0) {
            return;
        }
        for (File file : files) {
            String[] filename = file.getName().split(".");
            if (!filename[0].equals(this.tableName) || !filename[1].equals("data")) {
                continue;
            }
            ArrayList<Row> rows;
            try {
                rows = deserialize(file);
            } catch (Exception e) {
                throw new IOException();
            }
            for (Row row : rows) {
                ArrayList<Entry> entries = row.getEntries();
                for (int i = 0; i < entries.size(); i++) {
                    if (i == primaryIndex) {
                        index.put(entries.get(i), row);
                        break;
                    }
                }
            }
        }
    }

    public void insert(String[] columns, String[] values) {

    }

    public void delete() {
        // TODO
    }

    public void update() {
        // TODO
    }

    private void serialize() {
        // TODO
    }

    private ArrayList<Row> deserialize(File file) throws java.io.IOException, ClassNotFoundException {
        ObjectInputStream stream = new ObjectInputStream(new FileInputStream(file));
        ArrayList<Row> rows = (ArrayList<Row>) stream.readObject();
        stream.close();
        return rows;
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
