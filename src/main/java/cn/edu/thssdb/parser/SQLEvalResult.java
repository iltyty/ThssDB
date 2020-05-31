package cn.edu.thssdb.parser;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.Status;

import java.util.ArrayList;
import java.util.List;

public class SQLEvalResult {
    public String message;
    public Exception error;
    public QueryResult queryResult;

    public SQLEvalResult() {
        this.error = null;
        this.queryResult = null;
        this.message = null;
    }

    public SQLEvalResult(Exception error) {
        this.error = error;
        this.message = null;
        this.queryResult = null;
    }

    public boolean onError() {
        return this.error != null;
    }

    public void setQueryResult(QueryResult queryResult) {
        this.queryResult = queryResult;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
