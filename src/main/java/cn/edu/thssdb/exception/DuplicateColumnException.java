package cn.edu.thssdb.exception;

public class DuplicateColumnException extends RuntimeException {
    String columnName;

    public DuplicateColumnException(String name) {
        columnName = name;
    }

    @Override
    public String getMessage() {
        return columnName;
    }
}
