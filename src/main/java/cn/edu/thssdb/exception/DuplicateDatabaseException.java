package cn.edu.thssdb.exception;

public class DuplicateDatabaseException extends RuntimeException {
    String dbName;

    public DuplicateDatabaseException(String name) {
        dbName = name;
    }

    @Override
    public String getMessage() {
        return dbName;
    }
}
