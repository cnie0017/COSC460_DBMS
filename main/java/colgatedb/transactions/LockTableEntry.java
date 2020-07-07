package colgatedb.transactions;

import java.util.*;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */

/**
 * Represents the state associated with the lock on a particular page.
 */
public class LockTableEntry {

    private Permissions lockType;             // null if no one currently has a lock
    private Set<TransactionId> lockHolders;   // a set of txns currently holding a lock on this page
    private LinkedList<LockRequest> requests;       // a queue of outstanding requests
    private int countExclusive;             // a counter for exclusive lock requests

    public LockTableEntry() {
        lockType = null;
        lockHolders = new HashSet<>();
        requests = new LinkedList<>();
        countExclusive = 0;
    }


// some helper methods
    public Permissions getLockType(){
        return this.lockType;
    }

    public Set<TransactionId> getLockHolders(){ return this.lockHolders; }

    public void setLockType(Permissions p){ this.lockType = p; }

    public boolean noRequest(){ return requests.size() == 0; }

    //returns the first tid in requests queue
    public TransactionId getFirstInRequest(){
        return requests.getFirst().tid;
    }

    public void deleteRequest(TransactionId tid,Permissions perm){
        for (LockRequest req:requests){
            if (req.equals(new LockRequest(tid,perm))){
                requests.remove(req);
            }
        }
    }

    public boolean inRequest(TransactionId tid, Permissions perm){
        return requests.contains(new LockRequest(tid,perm));
    }

    //returns the lock type of a particular tid. Null if this tid not hold this lock
    public Permissions getLockTypeoftid(TransactionId tid){
        if (lockHolders.contains(tid)){
            return lockType;
        }
        return null;
    }

    //returns whether tid with perm can get this lock or not
    public synchronized boolean canGetLock(TransactionId tid, Permissions perm){
        if (perm == Permissions.READ_ONLY){
            if (lockType!=Permissions.READ_WRITE && countExclusive == 0){
                return true;
            }
        }
        else{ //wants exclusive lock
            if (lockHolders.contains(tid)&&lockHolders.size() == 1){
                // being the only one that is currently holding this lock
                return true;
            }
            else if (lockHolders.size() == 0){
                if (noRequest()){
                    return true;
                }
                else if (getFirstInRequest() == tid){
                //if the first one in requests
                    requests.pop();
                    countExclusive--;
                    return true;
                }
            }
        }
        return false;
    }

    public synchronized void addRequest(TransactionId tid, Permissions perm){
        LockRequest newrequest = new LockRequest(tid,perm);
        if (requests.contains(newrequest)){
            // if this request is already in requests queue
            return;
        }
        //give priority to upgrade
        if(lockHolders.contains(tid)) {
            if (lockType == Permissions.READ_ONLY && lockHolders.contains(tid) && perm.equals(Permissions.READ_WRITE)) {
                requests.addFirst(newrequest);
                countExclusive++;
            }
        }
        else{
            if (perm.equals(Permissions.READ_WRITE)){
                countExclusive++;
            }
            requests.add(newrequest);
        }
    }

    //give lock to qualified tid
    public synchronized void grantlock(TransactionId tid, Permissions perm){
        lockType = perm;
        if (!lockHolders.contains(tid)) {
            lockHolders.add(tid);
        }
        //remove this LockRequest from requests if necessary
        if (requests.contains(new LockRequest(tid,perm))){
            requests.pop();
            if (perm == Permissions.READ_WRITE){
                countExclusive--;
            }
        }

    }


    /**
     * A class representing a single lock request.  Simply tracks the txn and the desired lock type.
     * Feel free to use this, modify it, or not use it at all.
     */
    private class LockRequest {
        public final TransactionId tid;
        public final Permissions perm;

        public LockRequest(TransactionId tid, Permissions perm) {
            this.tid = tid;
            this.perm = perm;
        }

        public boolean equals(Object o) {
            if (!(o instanceof LockRequest)) {
                return false;
            }
            LockRequest otherLockRequest = (LockRequest) o;
            return tid.equals(otherLockRequest.tid) && perm.equals(otherLockRequest.perm);
        }

        public String toString() {
            return "Request[" + tid + "," + perm + "]";
        }
    }
}
