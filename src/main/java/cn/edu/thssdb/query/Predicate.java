package cn.edu.thssdb.query;

import cn.edu.thssdb.type.LogicalOpType;

public class Predicate {
    private Predicate lhs;
    private Predicate rhs;
    private boolean terminal;
    private LogicalOpType type;
    private Condition condition;

    public Predicate(Condition condition) {
        this.terminal = true;
        this.condition = condition;
    }

    public Predicate(Predicate lhs, Predicate rhs, LogicalOpType type) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.type = type;
        this.terminal = false;
    }

    public Predicate getLhs() {
        return lhs;
    }

    public Predicate getRhs() {
        return rhs;
    }

    public LogicalOpType getType() {
        return type;
    }

    public boolean isTerminal() {
        return terminal;
    }

    public Condition getCondition() {
        return condition;
    }
}
