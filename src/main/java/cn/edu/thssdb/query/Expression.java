package cn.edu.thssdb.query;

import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.type.NumericOpType;

public class Expression {
    private Expression lhs;
    private Expression rhs;
    private boolean terminal;
    private Comparer comparer;
    private NumericOpType type;

    public Expression(Comparer comparer) {
        this.terminal = true;
        this.comparer = comparer;
    }

    public Expression(NumericOpType type, Expression lhs, Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.type = type;
        this.terminal = false;
    }

    public Expression(Expression e) {
        this.lhs = e.lhs;
        this.rhs = e.rhs;
        this.type = e.type;
        this.comparer = e.comparer;
        this.terminal = e.terminal;
    }

    public Expression getLhs() {
        return lhs;
    }

    public Expression getRhs() {
        return rhs;
    }

    public NumericOpType getType() {
        return type;
    }

    public boolean isTerminal() {
        return terminal;
    }

    public Comparer getComparer() {
        return comparer;
    }

    public boolean isConst() {
        if (terminal) {
            return comparer.getType() != ComparerType.COLUMN;
        }
        return lhs.isConst() && rhs.isConst();
    }

    public boolean isSingleColumn() {
        return terminal && comparer.getType() == ComparerType.COLUMN;
    }
}
