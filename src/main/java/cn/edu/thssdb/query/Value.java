package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;

import java.util.function.Function;

public class Value {
    public Type type;
    public Comparable value;

    public enum Type {
        COLUMN, NUMBER, STRING, NULL
    }

    public Value(String value, Type type) {
        this.type = type;
        switch (type) {
            case STRING:
            case COLUMN:
                this.value = value;
                break;
            case NUMBER:
                this.value = Double.parseDouble(value);
                break;
            case NULL:
                this.value = null;
        }
    }
}
