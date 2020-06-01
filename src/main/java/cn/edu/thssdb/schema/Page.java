package cn.edu.thssdb.schema;

import java.util.ArrayList;

public class Page {
    public String path;
    public int size;
    public ArrayList<Entry> entries;
    public int id;
    public boolean dirty;

    public Page(String dbName, String tableName, int id) {
        this.id = id;
        path = "./" + dbName + "/" + tableName + "." + id + ".dat";
        size = 0;
        entries = new ArrayList<>();
        dirty = false;
    }

    public void insert(Entry entry, int size) {
        entries.add(entry);
        this.size += size;
    }

    public void delete(Entry entry, int size) {
        entries.remove(entry);
        this.size -= size;
    }

    public void updateSize(int old, int newSize) {
        this.size -= old;
        this.size += newSize;
    }
}
