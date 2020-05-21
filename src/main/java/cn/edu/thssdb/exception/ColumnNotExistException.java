package cn.edu.thssdb.exception;

public class ColumnNotExistException extends RuntimeException {
    @Override
    public String getMessage() { return "Exception: column does not exist!"; }
}
