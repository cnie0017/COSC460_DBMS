package colgatedb.transactions;

import colgatedb.page.PageId;

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
public class LockManagerImpl implements LockManager {

    private Map<PageId, LockTableEntry> locktable;//stores info about lock for each pid
    private WaitForGraph graph;// for deadlock detection
    private Map<TransactionId, Integer> timestamp;//records the first time each thread is in contact with lockmanager
    private int timeidx;

    public LockManagerImpl() {
        locktable = new HashMap<>();
        graph = new WaitForGraph();
        timestamp = new HashMap<>();
        timeidx = 0;
    }


//some helper functions
    //wait-die, younger ones die first
    private void waitDie(TransactionId tid, LockTableEntry entry, Permissions perm) throws TransactionAbortedException{
        boolean abort = false;
        for (TransactionId holder : entry.getLockHolders()) {
            int h = timestamp.get(holder);
            int t = timestamp.get(tid);
            if (h < t) {
                abort = true;
                break;
            }
        }
        if (abort) {//deletes everything about tid in request and graph
            entry.deleteRequest(tid, perm);
            graph.cleanup(tid);
            throw new TransactionAbortedException();
        }
        // otherwise, tid is an old transaction, cannot be aborted, it waits
    }

    //when tid try to acquire a lock for the first time, get the lock if it can acquire lock, wait otherwise
    private boolean acquireFirstTime(TransactionId tid, Permissions perm, LockTableEntry entry, boolean canAcquire) throws TransactionAbortedException{
        boolean waiting = true;
        if (canAcquire){
            entry.grantlock(tid, perm);
            waiting = false;
        }
        else {// have to wait
            entry.addRequest(tid, perm);
            //detect whether a deadlock occurs when adding tid into the graph
            boolean deadlock = graph.addTrans(tid, entry);
            if (deadlock) {
                //uses waitDie to choose victim and then abort it
                waitDie(tid,entry,perm);
            }
        }
        return waiting;
    }

    //when tid has been waiting for this lock and now is reacquiring
    private boolean reTryAcquire(TransactionId tid, Permissions perm, LockTableEntry entry, boolean canAcquire){
        boolean waiting = true;
        if (canAcquire){
            // removes tid from the wait for graph
            graph.cleanup(tid);
            entry.grantlock(tid, perm);
            waiting = false;
        }
        //otherwise, continue waiting
        return waiting;
    }


    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        boolean waiting = true;
        while (waiting){
            synchronized (this) {
                //create entry for pages in table
                if (!locktable.containsKey(pid)){
                    locktable.put(pid,new LockTableEntry());
                }
                //create timestamp for a new thread
                if(!timestamp.containsKey(tid)){
                    timestamp.put(tid,timeidx);
                    timeidx++;
                }
                LockTableEntry entry = locktable.get(pid);
                boolean canAcquire =  entry.canGetLock(tid,perm);
                if (!entry.inRequest(tid,perm)){
                    //when tid try to acquire this lock for the first time
                    waiting = acquireFirstTime(tid,perm,entry,canAcquire);
                }
                else{// have been waiting
                    waiting = reTryAcquire(tid,perm,entry,canAcquire);
                }
                if (waiting) {
                    try {
                        wait();
                    } catch (InterruptedException e) { }
                }
            }
        }
    }


    @Override
    // returns true if current lock is as strong as perm
    public synchronized boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        if (!locktable.containsKey(pid)||locktable.get(pid).getLockType() == null){
            return false;
        }
        LockTableEntry entry = locktable.get(pid);
        Permissions currentLock = entry.getLockTypeoftid(tid);
        if (currentLock == null){
            return false;
        }
        else {
            return perm == Permissions.READ_ONLY || perm == currentLock;
        }
    }

    @Override
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (!holdsLock(tid,pid,Permissions.READ_ONLY)){
            throw new LockManagerException("tid cannot release a lock without holding it!");
        }
        LockTableEntry entry = locktable.get(pid);
        Set<TransactionId> currentholders = entry.getLockHolders();
        currentholders.remove(tid);
        if (!entry.noRequest()){
            graph.cleanup(tid);
        }
        //reset the locktype to null if tid is the last one holding it
        if (currentholders.size() == 0){
            entry.setLockType(null);
        }
        notifyAll();
    }

    @Override
    public synchronized List<PageId> getPagesForTid(TransactionId tid) {
        List<PageId> allpages = new ArrayList<>();
        for (PageId pid:locktable.keySet()){
            Set<TransactionId> currentHolders = locktable.get(pid).getLockHolders();
            if (currentHolders.contains(tid)){
                allpages.add(pid);
            }
        }
        return allpages;
    }

    @Override
    public synchronized List<TransactionId> getTidsForPage(PageId pid) {
        List<TransactionId> tids = new ArrayList<>();
        tids.addAll(locktable.get(pid).getLockHolders());
        return tids;
    }

    // helper class for deadlock detection
    private class WaitForGraph{
        private Map<TransactionId,LinkedList<TransactionId>> edges;//stores info about each tid and a list of transactions that tid is waiting for
        private Map<TransactionId,Integer> incomingedges;//stores the number of in-degree for each tid

        private WaitForGraph(){
            edges = new HashMap<>();
            incomingedges = new HashMap<>();
        }

        // add the transaction to the graph. return true if this adding creates a deadlock
        private synchronized boolean addTrans(TransactionId tid, LockTableEntry entry){
            //add transaction as a new node into graph
            if (!edges.containsKey(tid)){
                edges.put(tid, new LinkedList<TransactionId>());
            }
            //add lock holder transactions into tid's adjacency list
            Iterator<TransactionId> lockholderite = entry.getLockHolders().iterator();
            while (lockholderite.hasNext()){
                TransactionId lockholder = lockholderite.next();
                // exclude those who are waiting for themselves
                if (!edges.get(tid).contains(lockholder)){
                    edges.get(tid).add(lockholder);
                }
                //add lockholders into incoming edges since there is an edge (tid,lockholder) for each of them
                if (lockholder != tid){
                    if (!incomingedges.containsKey(lockholder)){
                        incomingedges.put(lockholder,(Integer)1);
                    }
                    else{
                        incomingedges.replace(lockholder,incomingedges.get(lockholder)+1);
                    }
                }
            }
            if (!incomingedges.containsKey(tid)){
                incomingedges.put(tid,(Integer)0);
            }
            return detectDeadLock();
        }

        // add sources into queue for topological sort
        private void addSources(Queue<TransactionId> queue){
            for(TransactionId tid: incomingedges.keySet()) {
                if (incomingedges.get(tid).intValue()==0){
                    queue.add(tid);
                }
            }
        }

        // implements topological sort for the graph. decrementing the in-degrees of connected vertices to
        // find new sources, and add them into a topological sorted list
        private void topoSort(Queue<TransactionId> q, LinkedList<TransactionId> topoSortList){
            Map<TransactionId,Integer> incomingcopy = new HashMap<>(incomingedges);
            while (!q.isEmpty()){
                // get the first tid out from q
                TransactionId source = q.poll();
                topoSortList.add(source);
                //decrement incoming count for each vertex in source's adjacency list
                if (edges.containsKey(source)) {
                    for (TransactionId tid : edges.get(source)) {
                        if (!tid.equals(source)){
                            Integer i = incomingcopy.get(tid) - 1;
                            incomingcopy.replace(tid, i);
                            if (i.intValue() == 0) {
                                q.add(tid);// tid is a new source
                            }
                        }

                    }
                }
            }
        }

        //Detect deadlock in graph, return true if deadlock occurs by implementing a topological sort of graph
        private synchronized boolean detectDeadLock(){
            Queue<TransactionId> q = new LinkedList<TransactionId>();
            addSources(q);//add vertices without incoming edges to q
            LinkedList<TransactionId> topoSortList = new LinkedList<>();
            topoSort(q,topoSortList);
            //deadlock occurs if there is no topological order for this graph
            return topoSortList.size() != incomingedges.size();
        }

        /* cleanup everything about targettid in the graph:
           1.decrement the in-degrees of all neighbor vertices of targettid
           2.removing targettid from the outcoming list of any tid
           3.set in-degree of targettid in incomingedges to be zero
        */
        private synchronized void cleanup(TransactionId targettid){
            for (TransactionId tid : edges.keySet()) {
                LinkedList<TransactionId> outcomings = edges.get(tid);
                if (tid.equals(targettid)) {
                    for (TransactionId outtid : outcomings) {
                        //decrement the in-degree of reachable vertex of targettid by 1
                        if (!outtid.equals(targettid)) {
                            incomingedges.replace(outtid, incomingedges.get(outtid) - 1);
                        }
                    }
                }
                if (outcomings.contains(targettid)) {
                    outcomings.remove(targettid);
                }
            }
            incomingedges.replace(targettid, 0);
        }
    }
}
