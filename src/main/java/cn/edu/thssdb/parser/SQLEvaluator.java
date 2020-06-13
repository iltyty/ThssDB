package cn.edu.thssdb.parser;

import cn.edu.thssdb.schema.Manager;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.File;
import java.io.FileWriter;

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
            SQLEvalResult result = visitor.visitParse(parser.parse());
            if (manager.context.mutex.isLocked()) {
                // write log
                File logFile = new File("./" + manager.context.databaseName + "/.log");
                if (!logFile.exists()) {
                    logFile.createNewFile();
                }
                FileWriter writer = new FileWriter(logFile);
                writer.append(stmt).append('\n');
                writer.close();
            }
            return result;
        } catch (Exception e) {
            return new SQLEvalResult(e);
        }
    }
}
