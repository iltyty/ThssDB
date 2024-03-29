package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;

public class Column implements Comparable<Column> {
    private String name;
    private ColumnType type;
    private int primary;
    public boolean notNull;
    public int maxLength;

    public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
        this.name = name;
        this.type = type;
        this.primary = primary;
        this.notNull = notNull;
        this.maxLength = maxLength;
    }

    public int getPrimary() {
        return primary;
    }

    public void setPrimary(int primary) { this.primary = primary; }

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return type;
    }

    @Override
    public int compareTo(Column e) {
        return name.compareTo(e.name);
    }

    public String toString() {
        return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
    }
}
