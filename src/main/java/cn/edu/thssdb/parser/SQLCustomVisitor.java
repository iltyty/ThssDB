package cn.edu.thssdb.parser;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Manager;

import java.util.StringJoiner;

public class SQLCustomVisitor extends SQLBaseVisitor<String> implements SQLVisitor<String> {
    private Manager manager;

    public SQLCustomVisitor(Manager manager) {
        super();
        this.manager = manager;
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
        } else if (ctx.create_table_stmt() != null) {
            return visitCreate_table_stmt(ctx.create_table_stmt());
        } else {
            return "";
        }
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
            manager.switchDatabase(dbName, manager.getContext());
        } catch (Exception e) {
            return e.getMessage();
        }
        return String.format("Switched to database %s.", dbName);
    }

    @Override
    public String visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        String tbName = ctx.table_name().getText();
        return "";
    }
}
