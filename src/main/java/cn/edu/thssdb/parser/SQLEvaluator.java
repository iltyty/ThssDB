package cn.edu.thssdb.parser;

import cn.edu.thssdb.schema.Manager;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public class SQLEvaluator {
    private Manager manager;

    public SQLEvaluator(Manager manager) {
        this.manager = manager;
    }

    public SQLEvalResult evaluate(String stmt){
        SQLLexer lexer = new SQLLexer(CharStreams.fromString(stmt));
        SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
        // Hack ?
        manager.context.mutex.lock();
        manager.context.mutex.unlock();
        try {
            SQLCustomVisitor visitor = new SQLCustomVisitor(manager);
            return visitor.visitParse(parser.parse());
        } catch (Exception e) {
            return new SQLEvalResult(e);
        }
    }
}
