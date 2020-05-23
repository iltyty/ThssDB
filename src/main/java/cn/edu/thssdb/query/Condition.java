package cn.edu.thssdb.query;

import cn.edu.thssdb.type.ComparatorType;

public class Condition {
    private Expression lhs;
    private Expression rhs;
    private ComparatorType type;

    public Condition(Expression lhs, Expression rhs, ComparatorType type) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.type = type;
    }

    public Expression getLhs() {
        return lhs;
    }

    public Expression getRhs() {
        return rhs;
    }

    public ComparatorType getType() {
        return type;
    }
}
