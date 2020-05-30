package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;

import java.util.*;
import java.util.function.Predicate;

public abstract class QueryTable implements Iterator<Row> {
    protected Queue<Row> buffer;

    public ArrayList<Column> columns;
    public Predicate<Row> predicate;

    public abstract void putRowsToBuffer();

    public abstract void clear();

    public abstract void setWhere(Where where);

    public abstract ArrayList<MetaInfo> genMetaInfo();

    QueryTable() {
        buffer = new LinkedList<>();
    }

    @Override
    public boolean hasNext() {
        putRowsToBuffer();
        return !buffer.isEmpty();
    }

    @Override
    public Row next() {
        if (buffer.isEmpty()) {
            putRowsToBuffer();
        }
        if (buffer.isEmpty()) {
            return null;
        }
        Row row = buffer.poll();
        if (buffer.isEmpty()) {
            putRowsToBuffer();
        }
        return row;
    }
}