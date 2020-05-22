package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.exception.ValueException;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;

import java.util.StringJoiner;

public class SQLCustomVisitor extends SQLBaseVisitor {
    private Manager manager;

    public SQLCustomVisitor(Manager manager) {
        super();
        this.manager = manager;
    }

    private boolean equals(String columnName1, String columnName2) {
        return columnName1.toLowerCase().equals(columnName2.toLowerCase());
    }

    @Override
    public String visitParse(SQLParser.ParseContext ctx) {
        return visitSql_stmt_list(ctx.sql_stmt_list());
    }

    @Override
    public String visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        StringJoiner sj = new StringJoiner("\n\n");
        for (SQLParser.Sql_stmtContext c : ctx.sql_stmt()) {
            sj.add(visitSql_stmt(c));
        }
        return sj.toString();
    }

    @Override
    public String visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        if (ctx.create_db_stmt() != null) {
            return visitCreate_db_stmt(ctx.create_db_stmt());
        } else if (ctx.drop_db_stmt() != null) {
            return visitDrop_db_stmt(ctx.drop_db_stmt());
        } else if (ctx.use_db_stmt() != null) {
            return visitUse_db_stmt(ctx.use_db_stmt());
        } else if (ctx.create_table_stmt() != null) {
            return visitCreate_table_stmt(ctx.create_table_stmt());
        } else if (ctx.drop_table_stmt() != null) {
            return visitDrop_table_stmt(ctx.drop_table_stmt());
        } else if (ctx.show_table_stmt() != null) {
            return visitShow_table_stmt(ctx.show_table_stmt());
        } else if (ctx.insert_stmt() != null) {
            return visitInsert_stmt(ctx.insert_stmt());
        } else if (ctx.select_stmt() != null) {
            return visitSelect_stmt(ctx.select_stmt());
        }
        return null;
    }

    @Override
    public String visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        String dbName = ctx.database_name().getText();
        try {
            manager.createDatabase(dbName.toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return String.format("Database %s created.", dbName);
    }

    @Override
    public String visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        String dbName = ctx.database_name().getText();
        try {
            manager.deleteDatabase(dbName.toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return String.format("Database %s dropped.", dbName);
    }

    @Override
    public String visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        String dbName = ctx.database_name().getText();
        try {
            manager.switchDatabase(dbName);
        } catch (Exception e) {
            return e.getMessage();
        }
        return String.format("Switched to database %s.", dbName);
    }

    @Override
    public String visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        String tbName = ctx.table_name().getText();
        int n = ctx.column_def().size();

        // resolve columns definition
        Column[] columns = new Column[n];
        for (int i = 0; i < n; i++) {
            columns[i] = visitColumn_def(ctx.column_def().get(i));
        }

        // resolve table constraints
        if (ctx.table_constraint() != null) {
            String[] primaryNames = visitTable_constraint(ctx.table_constraint());
            int length = primaryNames.length;
            if (length >= 1) {
                for (String name : primaryNames) {
                    boolean exist = false;
                    for (Column column : columns) {
                        if (equals(column.getName(), name)) {
                            exist = true;
                            column.setPrimary(length == 1 ? 1 : 2);
                        }
                    }
                    if (!exist) {
                        throw new ColumnNotExistException(name);
                    }
                }
            }
        }

        try {
            manager.createTable(tbName, columns);
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Created table " + tbName + ".";
    }

    @Override
    public String visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        String tbName = ctx.table_name().getText();
        try {
            manager.deleteTable(tbName, ctx.K_IF() != null);
        } catch (Exception e) {
            return e.getMessage();
        }
        return String.format("Dropped table %s.", tbName);
    }

    @Override
    public String visitShow_table_stmt(SQLParser.Show_table_stmtContext ctx) {
        return manager.showTables(ctx.database_name().getText().toLowerCase());
    }

    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();
        String[] columnNames = null;
        if (ctx.column_name() != null && ctx.column_name().size() != 0) {
            columnNames = new String[ctx.column_name().size()];
            for (int i = 0; i < ctx.column_name().size(); i++) {
                columnNames[i] = ctx.column_name(i).getText().toLowerCase();
            }
        }
        for (SQLParser.Value_entryContext subCtx : ctx.value_entry()) {
            String[] values = visitValue_entry(subCtx);
            try {
                manager.insert(tableName, values, columnNames);
            } catch (Exception e) {
                return e.getMessage();
            }
        }
        return "Inserted " + ctx.value_entry().size() + " rows.";
    }

    @Override
    public String[] visitValue_entry(SQLParser.Value_entryContext ctx) {
        String[] values = new String[ctx.literal_value().size()];
        for (int i = 0; i < ctx.literal_value().size(); i++) {
            values[i] = ctx.literal_value(i).getText();
        }
        return values;
    }

    @Override
    public String visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        boolean distinct = false;
        if (ctx.K_DISTINCT() != null) {
            distinct = true;
        }
        int nColumn = ctx.result_column().size();
        String[] columnNames = new String[nColumn];
        for (int i = 0; i < nColumn; i++) {
            String columnName = ctx.result_column(i).getText().toLowerCase();
            if (columnName.equals("*")) {
                columnNames = null;
                break;
            }
            columnNames[i] = columnName;
        }
        int count = ctx.table_query().size();
        QueryTable[] queryTables = new QueryTable[count];
        try {
            for (int i = 0; i < count; i++) {
                queryTables[i] = visitTable_query(ctx.table_query(i));
            }
        } catch (Exception e) {
            return e.getMessage();
        }
        return "";
    }

    @Override
    public QueryTable visitTable_query(SQLParser.Table_queryContext ctx) {
        return null;
    }

    @Override
    public Column visitColumn_def(SQLParser.Column_defContext ctx) {
        boolean nonNull = false;
        int primary = 0;
        for (SQLParser.Column_constraintContext column_cons : ctx.column_constraint()) {
            String type = visitColumn_constraint(column_cons);
            if (type.equals("PRIMARY")) {
                primary = 1;
            } else if (type.equals("NONNULL")) {
                nonNull = true;
            }
            nonNull = nonNull || (primary > 0);
        }
        String name = ctx.column_name().getText().toLowerCase();
        Pair<ColumnType, Integer> type = visitType_name(ctx.type_name());
        ColumnType columnType = type.getKey();
        int maxLength = type.getValue();
        return new Column(name, columnType, primary, nonNull, maxLength);
    }

    @Override
    public String visitColumn_constraint(SQLParser.Column_constraintContext ctx) {
        if (ctx.K_PRIMARY() != null) {
            return "PRIMARY";
        } else if (ctx.K_NULL() != null) {
            return "NONNULL";
        }
        return null;
    }

    @Override
    public String[] visitTable_constraint(SQLParser.Table_constraintContext ctx) {
        int nColumn = ctx.column_name().size();
        String[] names = new String[nColumn];

        for (int i = 0; i < nColumn; i++) {
            names[i] = ctx.column_name(i).getText().toLowerCase();
        }
        return names;
    }

    @Override
    public Pair<ColumnType, Integer> visitType_name(SQLParser.Type_nameContext ctx) {
        if (ctx.T_INT() != null) {
            return new Pair<>(ColumnType.INT, -1);
        }
        if (ctx.T_LONG() != null) {
            return new Pair<>(ColumnType.LONG, -1);
        }
        if (ctx.T_FLOAT() != null) {
            return new Pair<>(ColumnType.FLOAT, -1);
        }
        if (ctx.T_DOUBLE() != null) {
            return new Pair<>(ColumnType.DOUBLE, -1);
        }
        if (ctx.T_STRING() != null) {
            try {
                return new Pair<>(ColumnType.STRING, Integer.parseInt(ctx.NUMERIC_LITERAL().getText()));
            } catch (Exception e) {
                throw new ValueException(e.getMessage());
            }
        }
        return null;
    }
}
