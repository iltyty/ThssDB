package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class QueryResult {

    private List<MetaInfo> metaInfoInfos;
    private List<Integer> index;

    public QueryResult(QueryTable[] queryTables) {
        this.index = new ArrayList<>();
        this.metaInfoInfos = new ArrayList<>();

        for (QueryTable table : queryTables) {
            this.metaInfoInfos.addAll(table.genMetaInfo());
        }
    }

    public static Row combineRow(LinkedList<Row> rows) {
        Row res = new Row();
        for (int i = rows.size() - 1; i >= 0; i--) {
            res.appendEntries(rows.get(i).getEntries());
        }
        return res;
    }

    public Row generateQueryRecord(Row row) {
        ArrayList<Entry> record = new ArrayList<>();
        for (int i : index) {
            record.add(row.getEntries().get(i));
        }
        return new Row(record.toArray(new Entry[index.size()]), -1);
    }
}