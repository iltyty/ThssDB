package cn.edu.thssdb.transaction;

import java.util.ArrayList;
import java.util.HashMap;

public class LockManager {
    private ArrayList<String> allLocks;
    // key: session(transaction) id
    // value: locks' name
    private HashMap<String, ArrayList<String>> lockTable;

    public boolean addLock(String sessionId, String lockName) {
        if (allLocks.contains(lockName)) {
            return false;
        }

        allLocks.add(lockName);
        if (lockTable.containsKey(sessionId)) {
            lockTable.get(sessionId).add(lockName);
        } else {
            ArrayList<String> list = new ArrayList<>();
            list.add(lockName);
            lockTable.put(sessionId, list);
        }
        return true;
    }

    public void releaseLocks(String sessionId) {
        lockTable.remove(sessionId);
    }

}
