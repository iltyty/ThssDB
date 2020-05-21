package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;
import java.util.Iterator;

public abstract class QueryTable implements Iterator<Row> {

    public ArrayList<Column> columns;

    QueryTable() {
        // TODO
    }

    public abstract ArrayList<MetaInfo> genMetaInfo();

    @Override
    public boolean hasNext() {

        return true;
    }

    @Override
    public Row next() {
        // TODO
        return null;
    }
}