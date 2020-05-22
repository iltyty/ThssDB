package cn.edu.thssdb.exception;

public class ColumnNotExistException extends RuntimeException {
    String name;

    public ColumnNotExistException(String name) { this.name = name; }

    @Override
    public String getMessage() {
        return String.format(
                "Exception: column%s does not exist!", name.isEmpty() ? "" : " " + name
        );
    }
}
