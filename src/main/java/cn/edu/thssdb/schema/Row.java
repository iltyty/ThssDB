package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

public class Row implements Serializable {
    private static final long serialVersionUID = -5809782578272943999L;
    protected ArrayList<Entry> entries;
    private int page;

    public Row() {
        this.entries = new ArrayList<>();
    }

    public Row(Entry[] entries, int page) {
        this.entries = new ArrayList<>(Arrays.asList(entries));
        this.page = page;
    }

    public ArrayList<Entry> getEntries() {
        return entries;
    }

    public void appendEntries(ArrayList<Entry> entries) {
        this.entries.addAll(entries);
    }

    public int getPage() {
        return page;
    }

    public String toString() {
        if (entries == null)
            return "EMPTY";
        StringJoiner sj = new StringJoiner(", ");
        for (Entry e : entries)
            sj.add(e.toString());
        return sj.toString();
    }

    public Comparable valueOf(int index) {
        Entry entry = entries.get(index);
        if (entry == null) {
            return null;
        }
        return entry.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Row row = (Row) o;

        return entries.equals(((Row) o).entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }
}
