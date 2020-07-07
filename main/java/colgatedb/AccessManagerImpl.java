package colgatedb;

import colgatedb.logging.LogFileRecovery;
import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;
import colgatedb.page.SimplePageId;
import colgatedb.transactions.*;

import java.io.IOException;
import java.util.*;

/**
 * ColgateDB
 *
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
public class AccessManagerImpl implements AccessManager {

    private boolean force = true;  // indicates whether force policy should be used
    private BufferManager bfmanager;
    private LockManager lockManager;
    private Map<PageId,List<TransactionId>> transList;
    private Map<PageId,List<TransactionId>> dirtyinfo;
    // a map that stores info about all <pid, a list of tids that have pinned pid>


    /**
     * Initialize the AccessManager, which includes creating a new LockManager.
     * @param bm buffer manager through which all page requests should be made
     */
    public AccessManagerImpl(BufferManager bm) {
        bfmanager = bm;
        lockManager = new LockManagerImpl();
        transList = new HashMap<>();
        dirtyinfo = new HashMap<>();
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        lockManager.acquireLock(tid,pid,perm);
    }

    @Override
    public boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        return lockManager.holdsLock(tid,pid,perm);
    }

    @Override
    public void releaseLock(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid,pid);
    }

    @Override
    public Page pinPage(TransactionId tid, PageId pid, PageMaker pageMaker) {
        synchronized (this){
            if (!transList.containsKey(pid)){
                ArrayList<TransactionId> l = new ArrayList<>();
                l.add(tid);
                transList.put(pid,l);
            }
            else{//already exist in transList
                List<TransactionId> repl = transList.get(pid);
                repl.add(tid);
                transList.replace(pid,repl);
            }
            return bfmanager.pinPage(pid,pageMaker);
        }
    }

    @Override
    public void unpinPage(TransactionId tid, Page page, boolean isDirty) {
        synchronized (this) {
            PageId pid = page.getId();
            if (isDirty){
                //write to log if this tid dirties the page
                List<TransactionId> dirtiedby;
                if (dirtyinfo.containsKey(pid)){
                    dirtiedby = dirtyinfo.get(pid);
                }
                else{
                    dirtiedby = new ArrayList<>();
                }
                if (!dirtiedby.contains(tid)){
                    dirtiedby.add(tid);
                }
                dirtyinfo.put(pid,dirtiedby);
                Database.getLogFile().logWrite(tid, page.getBeforeImage(), page);
            }
            bfmanager.unpinPage(pid, isDirty);
            //remove tid from the list for this page in transList
            List<TransactionId> del = transList.get(pid);
            del.remove(tid);
            transList.replace(pid,del);
        }
    }

    @Override
    public void allocatePage(PageId pid) {
        bfmanager.allocatePage(pid);
    }

    @Override
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    @Override
    public void transactionComplete(TransactionId tid, boolean commit) {
        // release lock for all the pages that tid currently has lock
        List<PageId> pages = lockManager.getPagesForTid(tid);
        for (PageId apage: pages){
            releaseLock(tid,apage);
        }
        // ensures strict 2PL
        for (PageId pid : transList.keySet()){
            List<TransactionId> tids = transList.get(pid);
            boolean dirty = bfmanager.isDirty(pid);
            if (commit && force){
                //If committing and force, pages dirtied by tid should be flushed to disk
                //update the before image of pages dirtied by this committed transaction
                Page p = bfmanager.getPage(pid);
                p.setBeforeImage();

                //flush log before flushing page under allow steal policy
                Database.getLogFile().force();
                bfmanager.flushPage(pid);
            }
            else if(!commit){
                // when aborting, pages that are still pinned should be unpinned and dirty ones should be discarded
                if (transList.get(pid).size()!=0){//still pinned
                    while (tids.contains(tid)){
                        //a page might be pinned by the same tid multiple times
                        tids.remove(tid);
                        bfmanager.unpinPage(pid,dirty);
                    }
                }
                if (dirtyinfo.containsKey(pid) && dirtyinfo.get(pid).contains(tid)){
                    //this pid is dirtied by this tid
                    bfmanager.discardPage(pid);
                    dirtyinfo.get(pid).remove(tid);
                }
            }
            transList.replace(pid,tids);//update transList after deleting tid
        }
    }

    @Override
    //force now becomes optional
    public void setForce(boolean force) {
        this.force = force;
    }
}
