package cn.edu.thssdb.exception;

public class IOException extends RuntimeException {
    String message;

    public IOException(String message) {
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
