package simpledb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

public class LockManager {
	
	private Map<PageId, Lock> locks;	
	private final int DEADLOCK_TIMEOUT = 250;
	
	public LockManager() {
		locks = new ConcurrentHashMap<PageId, Lock>();
	}
	
	public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
		String per = (perm == Permissions.READ_ONLY) ? "S" : "X";
		// System.out.print(tid.hashCode() + " acquiring " + per + " page " + pid.pageNumber());
		if (this.locks.containsKey(pid)) {
			Lock l = this.locks.get(pid);

			if (l.contains(tid)) { 

				if ((perm == Permissions.READ_ONLY) ||
					(perm == Permissions.READ_WRITE && l.isExclusive())) {
					// System.out.println(" " + tid.hashCode() + " already got " + per + " page " + pid.pageNumber());
					return;
				} else {
					// System.out.print(" " + tid.hashCode() + " requiring update");
					this.upgradeBlock(l, tid);
					l.type = Lock.X_LOCK;
					// System.out.println(" " + tid.hashCode() + " got update");
					return;
				}
			}

			if (l.isExclusive()) {
				block(pid);
				if (perm == Permissions.READ_ONLY) {
					this.locks.put(pid, new Lock(Lock.S_LOCK, tid));
				} else {
					this.locks.put(pid, new Lock(Lock.X_LOCK, tid));
				}
			} else {
				if (perm != Permissions.READ_ONLY) {
					block(pid);
					this.locks.put(pid, new Lock(Lock.X_LOCK, tid));
				} else {
					l.add(tid);
				}
			}
		} else {
			if (perm == Permissions.READ_ONLY) {
				this.locks.put(pid, new Lock(Lock.S_LOCK, tid));
			} else {
				this.locks.put(pid, new Lock(Lock.X_LOCK, tid));
			}
		}
		// System.out.println("  " + tid.hashCode() + " got " + per + " page " + pid.pageNumber());
	}

	public synchronized void upgradeBlock(Lock lock, TransactionId tid) throws TransactionAbortedException {
		try {
			long t0 = System.currentTimeMillis();
			while (!(lock.size() == 1 && lock.contains(tid))) {
				// System.out.print("-");
				Thread.sleep(10);
				if (System.currentTimeMillis() - t0 > DEADLOCK_TIMEOUT) { 
					// System.out.println("Upgrade Deadlock tid: " + tid.hashCode());
					throw new TransactionAbortedException();
				}
			}
		} catch (InterruptedException e) {}
	}

	public void releaseLock(TransactionId tid, PageId pid) {
		// System.out.println(tid.hashCode() + " release " + pid.pageNumber());
		if (this.locks.containsKey(pid)) {
			Lock l = this.locks.get(pid);

			if (l.isExclusive()) {
				this.locks.remove(pid);
			} else {
				l.remove(tid);
				if (l.isEmpty()) {
					this.locks.remove(pid);
				}
			}
		}
	}

	private synchronized void block(PageId pid) throws TransactionAbortedException {
		try {
			long t0 = System.currentTimeMillis();
			while (this.locks.containsKey(pid)) {
				// System.out.print("-");
				Thread.sleep(10);
				if (System.currentTimeMillis() - t0 > DEADLOCK_TIMEOUT) { 
					// System.out.println("Deadlock pid: " + pid.pageNumber());
					throw new TransactionAbortedException();
				}
			}
		} catch (InterruptedException e) {}
	}

    public boolean holdsLock(TransactionId tid, PageId pid) {
		Lock l = this.locks.get(pid);
		return l != null && l.contains(tid);
	}

	public ArrayList<PageId> releaseLocks(TransactionId tid) {
		ArrayList<PageId> released = new ArrayList<PageId>();

		for (PageId pid : this.locks.keySet()) {
			if (this.locks.get(pid).contains(tid)) {
				released.add(pid);
				this.releaseLock(tid, pid);
			}
		}

		return released;
	}

}
