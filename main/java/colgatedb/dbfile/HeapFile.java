package colgatedb.dbfile;

import colgatedb.*;
import colgatedb.page.*;
import colgatedb.transactions.Permissions;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.RecordId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

import java.util.Iterator;
import java.util.NoSuchElementException;

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
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with SlottedPage. The format of SlottedPages is described in the javadocs
 * for SlottedPage.
 *
 * @see SlottedPage
 */
public class HeapFile implements DbFile {

    private final SlottedPageMaker pageMaker;   // this should be initialized in constructor
    private int tableid;
    private TupleDesc td;
    private int pageSize;
    private int numPages;
    //private BufferManager bfmanager;
    private AccessManager accessManager;

    /**
     * Creates a heap file.
     * @param td the schema for records stored in this heapfile
     * @param pageSize the size in bytes of pages stored on disk (needed for PageMaker)
     * @param tableid the unique id for this table (needed to create appropriate page ids)
     * @param numPages size of this heapfile (i.e., number of pages already stored on disk)
     */
    public HeapFile(TupleDesc td, int pageSize, int tableid, int numPages) {
        this.td = td;
        this.pageSize = pageSize;
        this.tableid = tableid;
        this.numPages = numPages;
        pageMaker = new SlottedPageMaker(td,pageSize);
        accessManager = Database.getAccessManager();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return this.numPages;
    }

    @Override
    public int getId() {
        return this.tableid;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    /**
     A helper method that allocates space for a new page and insert tuple t into it
     */
    public void insertInToNewPage(TransactionId tid, Tuple t){
        SimplePageId newpid = new SimplePageId(tableid,numPages);
        // ensures atomicity when allocating a new page
        synchronized (this) {
            accessManager.allocatePage(newpid);
        }
        // ensures strict 2PL, acquire lock before pinning it
        try{
            accessManager.acquireLock(tid,newpid, Permissions.READ_WRITE);
        }
        catch (TransactionAbortedException e){ }
        SlottedPage newpage = (SlottedPage) accessManager.pinPage(tid,newpid,pageMaker);
        newpage.insertTuple(t);
        accessManager.unpinPage(tid,newpage,true);
        numPages++;
    }



    @Override
    public void insertTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        // find page with empty space to insert
        for (int i = 0; i < numPages; i++){
            SimplePageId pid = new SimplePageId(tableid,i);
            boolean justacquired = false;// whether the lock of this page is just acquired
            if (accessManager.holdsLock(tid,pid,Permissions.READ_ONLY)){
                justacquired = true;
            }
            // for strict 2PL, ensures lock is acquired before pinning
            try{
                accessManager.acquireLock(tid,pid, Permissions.READ_ONLY);
            }
            catch (TransactionAbortedException e){}
            SlottedPage apage = (SlottedPage) accessManager.pinPage(tid,pid,pageMaker);
            if (apage.getNumEmptySlots() > 0){// find empty slot in this page
                accessManager.acquireLock(tid,pid, Permissions.READ_WRITE);
                apage.insertTuple(t);
                accessManager.unpinPage(tid,apage,true);
                return;
            }
            else{//no empty room for insert into this page
                //if just acquired, can release it
                if (justacquired){
                    accessManager.releaseLock(tid,pid);
                }
                accessManager.unpinPage(tid,apage,true);
            }
        }
        // if no page or no page with empty space, needs to allocate space for a new page
        insertInToNewPage(tid,t);
    }


    @Override
    public void deleteTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        //get the target pid from t
        SimplePageId targetpid = (SimplePageId) t.getRecordId().getPageId();
        if (targetpid.getTableId() == tableid && targetpid.pageNumber() < numPages) {
            SlottedPage targetpage = (SlottedPage) accessManager.pinPage(tid,targetpid, pageMaker);
            targetpage.deleteTuple(t);
            accessManager.unpinPage(tid,targetpage, true);
        }
        else{
            throw new DbException("Cannot delete tuple!");
        }

    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }


    /**
     * @see DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {
        private int currPgNum;//Page number of the current page being used
        private SimplePageId currPid;
        private SlottedPage currPage;
        private Iterator<Tuple> tupleIt;//tuple iterator of the current page
        private TransactionId tid;
        private boolean open;

        public HeapFileIterator(TransactionId tid) {
            open = false;
            this.tid = tid;
        }

        @Override
        public void open() throws TransactionAbortedException {
            open = true;
            currPgNum = 0;
            currPid = new SimplePageId(tableid, currPgNum);
            currPage = (SlottedPage)accessManager.pinPage(tid,currPid,pageMaker);
            tupleIt = currPage.iterator();
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException {
            if (!open){
                return false;
            }
            if (tupleIt != null){
                // If tuples iterator for current page has more items
                if (tupleIt.hasNext()){
                    return true;
                }
                // Else, go on to the next page in the heap file
                else {
                    if (currPgNum < numPages-1) {
                        accessManager.unpinPage(tid, currPage,false);
                        currPgNum++;
                        currPid = new SimplePageId(tableid, currPgNum);
                        currPage = (SlottedPage) accessManager.pinPage(tid,currPid,pageMaker);
                        tupleIt = currPage.iterator();
                        return hasNext();
                    }
                    else{//No more pages, end of heap file
                        accessManager.unpinPage(tid,currPage,false);
                    }
                }
            }
            return false;
        }

        @Override
        public Tuple next() throws TransactionAbortedException, NoSuchElementException {
            if (!open){
                throw new NoSuchElementException("Closed!");
            }
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return tupleIt.next();
        }

        @Override
        public void rewind() throws TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            open = false;
            if (tupleIt.hasNext()){// If a page is still being referenced, should free it
                accessManager.unpinPage(tid,currPage,false);
            }
            currPgNum = -1;
            currPid = null;
            currPage = null;
            tupleIt = null;
        }
    }

}
