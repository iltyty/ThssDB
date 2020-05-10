package cn.edu.thssdb.exception;

public class DuplicatePrimaryException extends RuntimeException {
    String fieldName;

    public DuplicatePrimaryException(String name) {
        fieldName = name;
    }

    @Override
    public String getMessage() {
        return fieldName;
    }
}
