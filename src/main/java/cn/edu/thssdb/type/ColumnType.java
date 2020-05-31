package cn.edu.thssdb.type;

public enum ColumnType {
    INT, LONG, FLOAT, DOUBLE, STRING;

    public boolean isNumber() {
        return this != STRING;
    }
}
