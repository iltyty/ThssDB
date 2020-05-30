package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.function.Predicate;

public class Where {
    public Where left;
    public Where right;
    public Op op;
    public Cond cond;

    public enum Op {
        AND, OR
    }

    public Where(Where left, Where right, Op op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public Where(Cond cond) {
        this.cond = cond;
        this.right = null;
    }

    public boolean isTerminal() {
        return right == null;
    }

    public Predicate<Row> toPredicate(Table table) {
        if (isTerminal()) {
            return cond.toPredicate(table);
        } else {
            switch (op) {
                case OR:
                    return left.toPredicate(table).or(right.toPredicate(table));
                case AND:
                    return left.toPredicate(table).and(right.toPredicate(table));
                default:
                    return null;
            }
        }
    }
}
