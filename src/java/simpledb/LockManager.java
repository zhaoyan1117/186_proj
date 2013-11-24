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
		synchronized(pid){
			if (this.locks.containsKey(pid)) {
				Lock l = this.locks.get(pid);

				if (l.contains(tid)) { 

					if ((perm == Permissions.READ_ONLY) ||
						(perm == Permissions.READ_WRITE && l.isExclusive())) {
						return;
					} else {
						this.upgradeBlock(l, tid);
						l.type = Lock.X_LOCK;
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
		}
	}

	public void upgradeBlock(Lock lock, TransactionId tid) throws TransactionAbortedException {
		try {
			long t0 = System.currentTimeMillis();
			while (!(lock.size() == 1 && lock.contains(tid))) {
				Thread.sleep(10);
				if (System.currentTimeMillis() - t0 > DEADLOCK_TIMEOUT) { 
					throw new TransactionAbortedException();
				}
			}
		} catch (InterruptedException e) {}
	}

	public synchronized void releaseLock(TransactionId tid, PageId pid) {
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

	private void block(PageId pid) throws TransactionAbortedException {
		try {
			long t0 = System.currentTimeMillis();
			while (this.locks.containsKey(pid)) {
				Thread.sleep(10);
				if (System.currentTimeMillis() - t0 > DEADLOCK_TIMEOUT) { 
					throw new TransactionAbortedException();
				}
			}
		} catch (InterruptedException e) {}
	}

    public boolean holdsLock(TransactionId tid, PageId pid) {
		Lock l = this.locks.get(pid);
		return l != null && l.contains(tid);
	}

	public synchronized ArrayList<PageId> releaseLocks(TransactionId tid) {
		ArrayList<PageId> released = new ArrayList<PageId>();
		if (tid!=null){
			for (PageId pid : this.locks.keySet()) {
				if (this.locks.get(pid).contains(tid)) {
					released.add(pid);
					this.releaseLock(tid, pid);
				}
			}
		}

		return released;

	}

}
