package simpledb;

import java.util.*;

public class Lock {

    int type;
    List<TransactionId> tids;
    public static final int X_LOCK = 1;
    public static final int S_LOCK = 0;

    public Lock(int type, TransactionId tid) {
        this.type = type;
        this.tids = Collections.synchronizedList(new ArrayList<TransactionId>());
        tids.add(tid);
    }

    public boolean contains(TransactionId tid) {
        return tids.contains(tid);
    }

    public int size() {
        return tids.size();
    }

    public void remove(TransactionId tid) {
        if (tids.contains(tid)) {
            tids.remove(tid);
        }
    }

    public boolean isExclusive() {
    	return this.type == Lock.X_LOCK;
    }

    public boolean isEmpty() {
    	return this.tids.isEmpty();
    }

    public void add(TransactionId tid) {
    	if (type != Lock.S_LOCK) {
    		System.out.println("cao!!");
    		System.exit(0); 
    	}

    	this.tids.add(tid);
    }
}
