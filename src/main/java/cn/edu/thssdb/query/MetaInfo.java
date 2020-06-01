package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class MetaInfo {

    public String tableName;
    public List<Column> columns;

    public MetaInfo(String tableName, ArrayList<Column> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    int columnFind(String name) {
        int res = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (name.equals(columns.get(i).getName())) {
                res = i;
            }
        }
        return res;
    }
}