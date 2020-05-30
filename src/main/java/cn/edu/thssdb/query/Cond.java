package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.exception.ExprException;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class Cond {
    public Expr left;
    public Expr right;
    public Op op;

    public enum Op {
        EQ, NE, LE, LT, GE, GT
    }

    public Cond(Expr left, Expr right, Op op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public boolean evalConst() {
        // should be called when left and right are both const
        Value.Type t1 = left.evalConstType();
        Value.Type t2 = right.evalConstType();
        if (op == Op.EQ) {
            if (t1 == t2 || t1 == Value.Type.NULL || t2 == Value.Type.NULL) {
                return left.evalConst() == right.evalConst();
            }
            throw new ExprException();
        }
        if (op == Op.NE) {
            if (t1 == t2 || t1 == Value.Type.NULL || t2 == Value.Type.NULL) {
                return left.evalConst() != right.evalConst();
            }
            throw new ExprException();
        }
        Comparable v1 = left.evalConst();
        Comparable v2 = right.evalConst();
        if (v1 == null || v2 == null) {
            throw new ExprException();
        }
        if (op == Op.LT) {
            return v1.compareTo(v2) < 0;
        }
        if (op == Op.LE) {
            return v1.compareTo(v2) <= 0;
        }
        if (op == Op.GE) {
            return v1.compareTo(v2) >= 0;
        }
        if (op == Op.GT) {
            return v1.compareTo(v2) > 0;
        }
        return true;
    }

    public Predicate<Row> toPredicate(List<MetaInfo> metaInfos) {
        Function<Row, Comparable> e1 = left.extractor(metaInfos), e2 = right.extractor(metaInfos);
        // runtime reflection does not quite work for lambdas
        switch (op) {
            case EQ:
                return r -> {
                    Comparable v1 = e1.apply(r);
                    Comparable v2 = e2.apply(r);
                    if (v1 instanceof String != v2 instanceof String) {
                        throw new ExprException();
                    }
                    if (v1 instanceof String) {
                        return v1.equals(v2);
                    } else {
                        return ((Number) v1).doubleValue() == ((Number) v2).doubleValue();
                    }
                };
            case NE:
                return r -> {
                    Comparable v1 = e1.apply(r);
                    Comparable v2 = e2.apply(r);
                    if (v1 instanceof String != v2 instanceof String) {
                        throw new ExprException();
                    }
                    if (v1 instanceof String) {
                        return !v1.equals(v2);
                    } else {
                        return ((Number) v1).doubleValue() != ((Number) v2).doubleValue();
                    }
                };
            case LT:
                return r -> {
                    Comparable v1 = e1.apply(r);
                    Comparable v2 = e2.apply(r);
                    if (v1 instanceof String != v2 instanceof String) {
                        throw new ExprException();
                    }
                    if (v1 instanceof String) {
                        return v1.compareTo(v2) < 0;
                    } else {
                        return ((Number) v1).doubleValue() < ((Number) v2).doubleValue();
                    }
                };
            case LE:
                return r -> {
                    Comparable v1 = e1.apply(r);
                    Comparable v2 = e2.apply(r);
                    if (v1 instanceof String != v2 instanceof String) {
                        throw new ExprException();
                    }
                    if (v1 instanceof String) {
                        return v1.compareTo(v2) <= 0;
                    } else {
                        return ((Number) v1).doubleValue() <= ((Number) v2).doubleValue();
                    }
                };
            case GE:
                return r -> {
                    Comparable v1 = e1.apply(r);
                    Comparable v2 = e2.apply(r);
                    if (v1 instanceof String != v2 instanceof String) {
                        throw new ExprException();
                    }
                    if (v1 instanceof String) {
                        return v1.compareTo(v2) >= 0;
                    } else {
                        return ((Number) v1).doubleValue() >= ((Number) v2).doubleValue();
                    }
                };
            case GT:
                return r -> {
                    Comparable v1 = e1.apply(r);
                    Comparable v2 = e2.apply(r);
                    if (v1 instanceof String != v2 instanceof String) {
                        throw new ExprException();
                    }
                    if (v1 instanceof String) {
                        return v1.compareTo(v2) > 0;
                    } else {
                        return ((Number) v1).doubleValue() > ((Number) v2).doubleValue();
                    }
                };
            default:
                return null;
        }
    }
}
