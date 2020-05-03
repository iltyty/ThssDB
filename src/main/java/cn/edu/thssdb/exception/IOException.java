package cn.edu.thssdb.exception;

public class IOException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Exception: IO error returned from OS!";
    }
}
