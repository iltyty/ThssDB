package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;

public class SingleTable extends QueryTable implements Iterator<Row> {
    private Table table;
    private Iterator<Row> iterator;

    public SingleTable(Table table) {
        super();
        this.table = table;
        iterator = table.iterator();
        columns = table.columns;
    }

    @Override
    public void clear() {
        buffer.clear();
        iterator = table.iterator();
        putRowsToBuffer();
    }

    @Override
    public void setWhere(Where where) {
        if (where == null) {
            predicate = null;
        } else {
            predicate = where.toPredicate(table);
        }
    }

    @Override
    public ArrayList<MetaInfo> genMetaInfo() {
        return new ArrayList<MetaInfo>() {{
            add(new MetaInfo(table.tableName, table.columns));
        }};
    }

    @Override
    public void putRowsToBuffer() {
        if (predicate == null) {
            // no where clause
            if (iterator.hasNext()) {
                buffer.add(iterator.next());
            }
        } else {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (!predicate.test(row)) {
                    continue;
                }
                buffer.add(row);
                break;
            }
        }
    }

}
