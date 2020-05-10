package cn.edu.thssdb.exception;

public class DuplicateTableException extends RuntimeException {
    String tableName;

    public DuplicateTableException(String name) {
        tableName = name;
    }

    @Override
    public String getMessage() {
        return tableName;
    }
}
