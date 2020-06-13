package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;

public class SingleTable extends QueryTable implements Iterator<Row> {
    private Table table;
    private Iterator<Row> iterator;
    private String tableName;

    public SingleTable(Table table, String alias) {
        super();
        this.table = table;
        if (alias != null) {
            tableName = alias;
        } else {
            tableName = table.tableName;
        }
        iterator = table.iterator();
        columns = table.columns;
        putRowsToBuffer();
    }

    @Override
    public void clear() {
        buffer.clear();
        iterator = table.iterator();
        putRowsToBuffer();
    }

    @Override
    public ArrayList<MetaInfo> genMetaInfo() {
        return new ArrayList<MetaInfo>() {{
            add(new MetaInfo(tableName, table.columns));
        }};
    }

    @Override
    public void putRowsToBuffer() {
        if (iterator.hasNext()) {
            buffer.add(iterator.next());
        }
    }

}
