package cn.edu.thssdb.utils;

import cn.edu.thssdb.exception.ValueException;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.type.ColumnType;

public class Global {
  public static int fanout = 129;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;

  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667;

  public static int PAGE_SIZE = 1 << 12;

  public static String CLI_PREFIX = "ThssDB>";
  public static final String SHOW_TIME = "show time;";
  public static final String QUIT = "quit;";
  public static final String CONNECT = "connect;";

  public static final String S_URL_INTERNAL = "jdbc:default:connection";

  public static final String ADMIN_DB_NAME = "admin";

  public static Comparable comparableToColumnType(Comparable value, Column column) {
    switch (column.getType()) {
      case STRING:
        if (!(value instanceof String)) {
          throw new ValueException("Column type mismatch with new value");
        }
        if (((String) value).length() > column.maxLength) {
          throw new ValueException("Value length exceeds column max length");
        }
        return value;
      case LONG:
        if (value instanceof String) {
          throw new ValueException("Column type mismatch with new value");
        }
        return ((Number) value).longValue();
      case INT:
        if (value instanceof String) {
          throw new ValueException("Column type mismatch with new value");
        }
        return ((Number) value).intValue();
      case FLOAT:
        if (value instanceof String) {
          throw new ValueException("Column type mismatch with new value");
        }
        return ((Number) value).floatValue();
      case DOUBLE:
        if (value instanceof String) {
          throw new ValueException("Column type mismatch with new value");
        }
        return ((Number) value).doubleValue();
      default:
        return null;
    }
  }
}
