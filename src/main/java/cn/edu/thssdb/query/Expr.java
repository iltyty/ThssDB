package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.exception.ExprException;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.lang.reflect.Method;
import java.util.function.Function;

public class Expr {
    public Expr left;
    public Expr right;
    public Op op;
    public Value value;

    public enum Op {
        ADD, SUB, MUL, DIV
    }

    public Expr(Expr left, Expr right, Op op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public Expr(Value value) {
        this.value = value;
        this.right = null;
    }

    public boolean isTerminal() {
        return this.right == null;
    }

    public boolean isColumnName() {
        return isTerminal() && value.type == Value.Type.COLUMN;
    }

    public boolean isConst() {
        return isTerminal() ? value.type != Value.Type.COLUMN : (left.isConst() && right.isConst());
    }

    public Comparable evalConst() {
        // WARNING: should be called only when this is constexpr
        if (isTerminal()) {
            if (value.type == Value.Type.COLUMN) {
                throw new ExprException();
            }
            return value.value;
        } else {
            Comparable v1 = left.evalConst();
            Comparable v2 = right.evalConst();
            if (v1 == null || v2 == null) {
                throw new ExprException();
            }
            double d1 = ((Number) v1).doubleValue();
            double d2 = ((Number) v2).doubleValue();
            switch (op) {
                case ADD:
                    return d1 + d2;
                case SUB:
                    return d1 - d2;
                case MUL:
                    return d1 * d2;
                case DIV:
                    return d1 / d2;
            }
        }
        return null;
    }

    public Value.Type evalConstType() {
        // WARNING: should be called only when this is constexpr
        if (isTerminal()) {
            if (value.type == Value.Type.COLUMN) {
                throw new ExprException();
            }
            return value.type;
        } else {
            Value.Type t1 = left.evalConstType();
            Value.Type t2 = right.evalConstType();
            if (t1 == Value.Type.NUMBER && t2 == Value.Type.NUMBER) {
                return Value.Type.NUMBER;
            }
            throw new ExprException();
        }
    }

    public Function<Row, Comparable> extractor(Table table) {
        if (isTerminal()) {
            if (isColumnName()) {
                String name = (String) value.value;
                int index = table.findColumnIndex(name);
                if (index == -1) {
                    throw new ColumnNotExistException(name);
                }
                return r -> r.valueOf(index);
            } else {
                return r -> value.value;
            }
        } else {
            Function<Row, Comparable> e1 = left.extractor(table), e2 = right.extractor(table);
            switch (op) {
                case ADD:
                    return r -> {
                        Comparable v1 = e1.apply(r);
                        Comparable v2 = e2.apply(r);
                        if (v1 instanceof String || v2 instanceof String) {
                            throw new ExprException();
                        }
                        return ((Number) e1.apply(r)).doubleValue() + ((Number) e2.apply(r)).doubleValue();
                    };
                case SUB:
                    return r -> {
                        Comparable v1 = e1.apply(r);
                        Comparable v2 = e2.apply(r);
                        if (v1 instanceof String || v2 instanceof String) {
                            throw new ExprException();
                        }
                        return ((Number) e1.apply(r)).doubleValue() - ((Number) e2.apply(r)).doubleValue();
                    };
                case MUL:
                    return r -> {
                        Comparable v1 = e1.apply(r);
                        Comparable v2 = e2.apply(r);
                        if (v1 instanceof String || v2 instanceof String) {
                            throw new ExprException();
                        }
                        return ((Number) e1.apply(r)).doubleValue() * ((Number) e2.apply(r)).doubleValue();
                    };
                default:
                    return r -> {
                        Comparable v1 = e1.apply(r);
                        Comparable v2 = e2.apply(r);
                        if (v1 instanceof String || v2 instanceof String) {
                            throw new ExprException();
                        }
                        return ((Number) e1.apply(r)).doubleValue() / ((Number) e2.apply(r)).doubleValue();
                    };
            }
        }
    }
}
