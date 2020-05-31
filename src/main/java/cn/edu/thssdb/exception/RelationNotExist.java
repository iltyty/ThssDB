package cn.edu.thssdb.exception;

public class RelationNotExist extends RuntimeException {
    private String name;

    public RelationNotExist(String name) {
        this.name = name;
    }

    @Override
    public String getMessage() {
        return String.format("Relation '%s' does not exist", name);
    }
}
