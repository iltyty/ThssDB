package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class JointTable extends QueryTable implements Iterator<Row> {

    private List<Iterator<Row>> iterators;
    private List<Table> tables;
    private LinkedList<Row> rows;
    private Predicate<Row> join;
    private ArrayList<MetaInfo> metaInfos;

    public JointTable(List<Table> tables, Where join) {
        super();
        this.tables = tables;
        this.iterators = new ArrayList<>();
        this.rows = new LinkedList<>();
        this.columns = new ArrayList<>();
        for (Table t : tables) {
            this.columns.addAll(t.columns);
            this.iterators.add(t.iterator());
        }
        metaInfos = tables.stream()
                .map(table -> new MetaInfo(table.tableName, table.columns))
                .collect(Collectors.toCollection(ArrayList::new));
        this.join = join.toPredicate(metaInfos);
        putRowsToBuffer();
    }

    @Override
    public void putRowsToBuffer() {
        while (true) {
            Row row = buildNewRow();
            if (row == null) {
                return;
            }
            if (!join.test(row)) {
                continue;
            }
            buffer.add(row);
            return;
        }
    }

    private Row buildNewRow() {
        if (rows.isEmpty()) {
            for (Iterator<Row> iter : iterators) {
                if (!iter.hasNext()) {
                    return null;
                }
                rows.push(iter.next());
            }
        } else {
            int index;
            for (index = iterators.size() - 1; index >= 0; index--) {
                rows.pop();
                if (!iterators.get(index).hasNext()) {
                    iterators.set(index, tables.get(index).iterator());
                } else {
                    break;
                }
            }
            if (index < 0) {
                return null;
            }
            for (int i = index; i < iterators.size(); i++) {
                if (!iterators.get(i).hasNext()) {
                    return null;
                }
                rows.push(iterators.get(i).next());
            }
        }
        return QueryResult.combineRow(rows);
    }

    @Override
    public void clear() {
        for (int i = 0; i < tables.size(); i++) {
            iterators.set(i, tables.get(i).iterator());
        }
        putRowsToBuffer();
    }

    @Override
    public ArrayList<MetaInfo> genMetaInfo() {
        return metaInfos;
    }
}
