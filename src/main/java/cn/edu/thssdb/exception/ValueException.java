package cn.edu.thssdb.exception;

public class ValueException extends RuntimeException {
    private String message;

    public ValueException(String message) {
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
