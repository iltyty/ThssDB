package cn.edu.thssdb.query;

import cn.edu.thssdb.type.ComparerType;

public class Comparer {
    private Comparable value;
    private ComparerType type;

    public Comparer(ComparerType type, String value) {
        this.type = type;
        switch (type) {
            case NUMBER:
                this.value = Double.parseDouble(value);
                break;
            default:
                this.value = value;
                break;
        }
    }

    public ComparerType getType() {
        return type;
    }

    public Comparable getValue() {
        return value;
    }

}
