package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.SQLEvalResult;
import cn.edu.thssdb.parser.SQLEvaluator;
import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.Date;

public class IServiceHandler implements IService.Iface {

    @Override
    public GetTimeResp getTime(GetTimeReq req) throws TException {
        GetTimeResp resp = new GetTimeResp();
        resp.setTime(new Date().toString());
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        return resp;
    }

    @Override
    public ConnectResp connect(ConnectReq req) throws TException {
        // TODO
        ConnectResp resp = new ConnectResp();
        if (req.username.equals("username") && req.password.equals("password")) {
            resp.setSessionId(req.hashCode());
            resp.setStatus(new Status(Global.SUCCESS_CODE));
            return resp;
        } else {
            resp.setSessionId(0);
            resp.setStatus(new Status(Global.FAILURE_CODE));
            return resp;
        }
    }

    @Override
    public DisconnectResp disconnect(DisconnectReq req) throws TException {
        DisconnectResp resp = new DisconnectResp();
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        return resp;
    }

    @Override
    public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
        ExecuteStatementResp resp = new ExecuteStatementResp();
        SQLEvalResult result = ThssDB.getInstance().getEvaluator().evaluate(req.statement);
        if (result.onError()) {
            resp.setHasResult(false);
            resp.setIsAbort(true);
            Status status = new Status();
            status.setCode(-1);
            status.setMsg(result.error.getMessage());
            resp.setStatus(status);
        } else {
            resp.setHasResult(true);
            resp.setIsAbort(false);
            Status status = new Status();
            status.setCode(0);
            status.setMsg(result.message);
            resp.setStatus(status);
            if (result.queryResult != null) {
                resp.setColumnsList(result.queryResult.columnsToString());
                resp.setRowList(result.queryResult.rowsToString());
            }
        }
        return resp;
    }
}
