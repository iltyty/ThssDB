package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.exception.DuplicateColumnException;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;

import java.util.*;
import java.util.stream.Collectors;

public class QueryResult {
    private List<Integer> indices;
    private List<MetaInfo> metaInfos;

    public String[] columnNames;
    public HashSet<Row> distinctResult;
    public List<Row> result;
    public boolean distinct;
    public boolean wildcard = false;
    public int count = 0;

    public QueryResult(QueryTable[] queryTables, String[] columnNames, boolean distinct) {
        if (distinct) {
            this.distinctResult = new HashSet<>();
        } else {
            this.result = new ArrayList<>();
        }
        this.distinct = distinct;
        this.columnNames = columnNames;

        metaInfos = new ArrayList<>();
        for (QueryTable queryTable : queryTables) {
            metaInfos.addAll(queryTable.genMetaInfo());
        }

        if (columnNames == null) {
            // wildcard
            wildcard = true;
            return;
        }

        indices = new ArrayList<>();
        // find columns from columnNames
        for (String name : columnNames) {
            int offset = 0;
            if (name.contains(".")) {
                String[] splits = name.split("\\.");
                if (splits.length != 2) {
                    throw new ColumnNotExistException(name);
                }
                String tableName = splits[0], columnName = splits[1];
                boolean found = false;
                for (MetaInfo meta : metaInfos) {
                    if (meta.tableName.equals(tableName)) {
                        int index = meta.columnFind(name);
                        if (index != -1) {
                            found = true;
                            indices.add(offset + index);
                            break;
                        }
                    }
                    offset += meta.columns.size();
                }
                if (!found) {
                    throw new ColumnNotExistException(name);
                }
            } else {
                int matchCount = 0;
                for (MetaInfo meta : metaInfos) {
                    int index = meta.columnFind(name);
                    if (index != -1) {
                        matchCount++;
                        if (matchCount > 1) {
                            throw new DuplicateColumnException(name);
                        }
                        indices.add(offset + index);
                    }
                    offset += meta.columns.size();
                }
                if (matchCount == 0) {
                    throw new ColumnNotExistException(name);
                }
            }
        }
    }

    public void addRow(List<Row> rows) {
        Row row = QueryResult.combineRow(rows);
        row = generateQueryRecord(row);
        count++;
        if (distinct) {
            distinctResult.add(row);
        } else {
            result.add(row);
        }
    }

    public static Row combineRow(List<Row> rows) {
        Row res = new Row();
        for (int i = rows.size() - 1; i >= 0; i--) {
            res.appendEntries(rows.get(i).getEntries());
        }
        return res;
    }

    public Row generateQueryRecord(Row row) {
        if (wildcard) {
            return row;
        }
        ArrayList<Entry> record = new ArrayList<>();
        for (int i : indices) {
            record.add(row.getEntries().get(i));
        }
        return new Row(record.toArray(new Entry[indices.size()]), -1);
    }

    public List<String> columnsToString() {
        if (wildcard) {
            return metaInfos.stream()
                    .flatMap(metaInfo -> metaInfo.columns
                            .stream()
                            .map(column -> metaInfo.tableName + "." + column.getName()))
                    .collect(Collectors.toList());
        } else {
            return Arrays.asList(columnNames);
        }
    }

    public List<List<String>> rowsToString() {
        if (distinct) {
            return distinctResult.stream()
                    .map(row -> row.getEntries().stream()
                            .map(entry -> entry.value.toString())
                            .collect(Collectors.toList()))
                    .collect(Collectors.toList());
        } else {
            return result.stream()
                    .map(row -> row.getEntries().stream()
                            .map(entry -> entry.value.toString())
                            .collect(Collectors.toList()))
                    .collect(Collectors.toList());
        }
    }
}