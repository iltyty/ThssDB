package cn.edu.thssdb.exception;

public class DuplicateTableException extends RuntimeException {
    String tableName;

    public DuplicateTableException(String name) {
        tableName = name;
    }

    @Override
    public String getMessage() {
        return String.format("Table %s already exists", tableName);
    }
}
