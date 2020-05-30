package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;

import java.util.*;

public abstract class QueryTable implements Iterator<Row> {
    protected Queue<Row> buffer;

    public ArrayList<Column> columns;

    public abstract void putRowsToBuffer();

    public abstract void clear();

    public abstract ArrayList<MetaInfo> genMetaInfo();

    QueryTable() {
        buffer = new LinkedList<>();
    }

    @Override
    public boolean hasNext() {
        return !buffer.isEmpty();
    }

    @Override
    public Row next() {
        if (buffer.isEmpty()) {
            return null;
        }
        Row row = buffer.poll();
        putRowsToBuffer();
        return row;
    }
}