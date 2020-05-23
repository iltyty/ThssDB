package cn.edu.thssdb.exception;

public class InvalidComparisionException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Exception: invalid comparision.";
    }
}
