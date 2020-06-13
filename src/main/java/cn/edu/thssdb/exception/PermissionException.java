package cn.edu.thssdb.exception;

public class PermissionException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Exception: operation not permitted";
    }
}
