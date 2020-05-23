package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ComparatorType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

public abstract class QueryTable implements Iterator<Row> {
    protected boolean isFirst;
    protected Predicate predicate;
    protected LinkedList<Row> queue;
    protected LinkedList<Row> buffer;
    public ArrayList<Column> columns;

    QueryTable() {
        this.isFirst = true;
        this.queue = new LinkedList<>();
        this.buffer = new LinkedList<>();
    }

    public void setPredicate(Predicate predicate) {
        this.predicate = predicate;
    }

    protected int find(ArrayList<Column> columns, String name) {
        int pos = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (name.equals(columns.get(i).getName())) {
                pos = i;
            }
        }
        if (pos == -1) {
            throw new ColumnNotExistException(name);
        }
        return pos;
    }

    protected Condition swapCondition(Condition cd) {
        ComparatorType type = cd.getType();
        Expression lhs = new Expression(cd.getRhs());
        Expression rhs = new Expression(cd.getLhs());
        switch (cd.getType()) {
            case LE:
                type = ComparatorType.GE;
                break;
            case LT:
                type = ComparatorType.GT;
                break;
            case GE:
                type = ComparatorType.LE;
                break;
            case GT:
                type = ComparatorType.LT;
                break;
        }
        return new Condition(lhs, rhs, type);
    }

    public abstract void reset();

    public abstract void figure();

    public abstract ArrayList<MetaInfo> genMetaInfo();

    @Override
    public boolean hasNext() {
        return isFirst || !buffer.isEmpty() || !queue.isEmpty();
    }

    @Override
    public Row next() {
        if (buffer.isEmpty()) {
            if (isFirst) {
                figure();
                isFirst = false;
            }
            while (!queue.isEmpty()) {
                buffer.add(queue.poll());
            }
            figure();
        }
        if (buffer.isEmpty()) {
            return null;
        }
        return buffer.poll();
    }
}