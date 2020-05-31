package cn.edu.thssdb.exception;

public class ExprException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Invalid expression type";
    }
}
