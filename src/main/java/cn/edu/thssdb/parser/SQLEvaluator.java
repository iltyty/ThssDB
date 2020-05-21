package cn.edu.thssdb.parser;

import cn.edu.thssdb.schema.Manager;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public class SQLEvaluator {
    private Manager manager;

    public SQLEvaluator(Manager manager) {
        this.manager = manager;
    }

    public String evaluate(String stmt){
        SQLLexer lexer = new SQLLexer(CharStreams.fromString(stmt));
        SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
        try {
            SQLCustomVisitor visitor = new SQLCustomVisitor(manager);
            return String.valueOf(visitor.visitParse(parser.parse()));
        } catch (Exception e) {
            return "Exception: illegal SQL statement." + e.getMessage();
        }
    }
}
