package colgatedb.page;

import colgatedb.Database;
import colgatedb.tuple.RecordId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
 * SlottedPage stores a collection of fixed-length tuples, all having the same schema.
 * Upon insertion, a tuple is assigned to a slot.  The number of slots available depends on
 * the size of the page and the schema of the tuples.
 */
public class SlottedPage implements Page {

    private final PageId pid;
    private final TupleDesc td;
    private final int pageSize;
    private Tuple[] tuplearr;
    private int slotnum;
    private int emptyslot;// added after feedback
    private final Byte oldDataLock = (byte) 0;
    byte[] oldData;
    // ------------------------------------------------

    /**
     * Constructs empty SlottedPage
     * @param pid  page id to assign to this page
     * @param td   the schema for tuples held on this page
     * @param pageSize the size of this page
     */
    public SlottedPage(PageId pid, TupleDesc td, int pageSize) {
        this.pid = pid;
        this.td = td;
        this.pageSize = pageSize;
        slotnum = (int)Math.floor((pageSize * 8.0)/(td.getSize()*8+1));
        emptyslot = slotnum;
        tuplearr = new Tuple[slotnum];
        setBeforeImage();  // used for logging, leave this line at end of constructor
    }

    /**
     * Constructs SlottedPage with its data initialized according to last parameter
     * @param pid  page id to assign to this page
     * @param td   the schema for tuples held on this page
     * @param pageSize the size of this page
     * @param data data with which to initialize page content
     */
    public SlottedPage(PageId pid, TupleDesc td, int pageSize, byte[] data) {
        this(pid, td, pageSize);
        setPageData(data);
        setBeforeImage();  // used for logging, leave this line at end of constructor
    }

    /**
     * A newly added constructor for lab11
     */
    public SlottedPage(PageId pid, byte[] bytes) {
        this(pid, Database.getCatalog().getTupleDesc(pid.getTableId()), bytes.length, bytes);
    }

    @Override
    public PageId getId() {
        return pid;
    }

    /**
     * @param slotno the slot number
     * @return true if this slot is used (i.e., is occupied by a Tuple).
     */
    public boolean isSlotUsed(int slotno) {
        if (tuplearr[slotno] == null){
                return false;
        }
        return true;
    }


    /**
     *
     * @param slotno the slot number
     * @return true if this slot is empty (i.e., is not occupied by a Tuple).
     */
    public boolean isSlotEmpty(int slotno) {
        return !isSlotUsed(slotno);
    }

    /**
     * @return the number of slots this page can hold.  Determined by
     * the page size and the schema (TupleDesc).
     */
    public int getNumSlots() {
        return slotnum;
    }

    /**
     * @return the number of slots on this page that are empty.
     */
    public int getNumEmptySlots() {
        //  version before submission: for loop check if empty
        // for Efficiency: maintain a counter variable rather than loop.
        return emptyslot;
    }

    /**
     * @param slotno the slot of interest
     * @return returns the Tuple at given slot
     * @throws PageException if slot is empty
     */
    public Tuple getTuple(int slotno) {
        if (isSlotUsed(slotno)){
            return tuplearr[slotno];
        }
        throw new PageException("An empty slot!");
    }

    /**
     * Adds the specified tuple to specific slot in page.
     * <p>
     * The tuple should be updated to reflect that it is now stored on this page
     * (hint: set its RecordId).
     *
     * @param slotno the slot into which this tuple should be inserted
     * @param t The tuple to add.
     * @throws PageException if the slot is full or TupleDesc of
     *                          passed tuple is a mismatch with TupleDesc of this page.
     */
    public void insertTuple(int slotno, Tuple t) {
        if (td.equals(t.getTupleDesc()) && isSlotEmpty(slotno)){
            RecordId newid = new RecordId(pid,slotno);
            t.setRecordId(newid);
            tuplearr[slotno] = t;
            emptyslot--;
            return;
        }
        throw new PageException("Unable to insert into "+slotno+"!");
    }

    /**
     * Adds the specified tuple to the page into an available slot.
     * <p>
     * The tuple should be updated to reflect that it is now stored on this page
     * (hint: set its RecordId).
     *
     * @param t The tuple to add.
     * @throws PageException if the page is full (no empty slots) or TupleDesc of
     *                          passed tuple is a mismatch with TupleDesc of this page.
     */
    public void insertTuple(Tuple t) throws PageException {
        if (td.equals(t.getTupleDesc())){
            for (int i = 0; i < tuplearr.length; i++){
                if (isSlotEmpty(i)){
                    insertTuple(i,t);
                    return;
                }
            }
        }
        throw new PageException("Unable to insert!");
    }

    /**
     * Delete the specified tuple from the page; the tuple should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws PageException if this tuple doesn't have a record id, is not on this page, or tuple
     *                          slot is already empty.
     */
    public void deleteTuple(Tuple t) throws PageException {
        // Feedback: Just set the tuple's RecordId to be null, don't make a new RecordId object.
        RecordId tid = t.getRecordId();
        if (tid != null && tid.getPageId() == pid && isSlotUsed(tid.tupleno())){
            tuplearr[tid.tupleno()] = null;
            emptyslot++;
            // version before submission: RecordId newid = new RecordId(null,0);
            // after feedback
            t.setRecordId(null);
            return;
        }
        throw new PageException("Unable to delete tuple!");
    }


    /**
     * Creates an iterator over the (non-empty) slots of the page.
     *
     * @return an iterator over all tuples on this page
     * (Note: calling remove on this iterator throws an UnsupportedOperationException)
     */
    public Iterator<Tuple> iterator() {
        return new MyIterator();
    }

    class MyIterator implements Iterator<Tuple> {

        private int currIdx;

        public MyIterator() {
            currIdx = 0;
        }

        @Override
        public boolean hasNext() {
            // feedback: sometimes returns true when answer is false (e.g., an empty page).
            // Should call findNextIdx in this
            // version before submisson:
            /*int i = findNextIdx(currIdx);
            return i < tuplearr.length && i >= 0;*/
            if (findNextIdx(currIdx) < tuplearr.length){
                return true;
            }
            return false;
        }

        public int findNextIdx(int idx){
            //find the next idx which the slot is not empty
            while (idx < tuplearr.length && isSlotEmpty(idx)) {
                idx++;
            }
            return idx;
        }
        @Override
        public Tuple next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Tuple result;
            if (isSlotUsed(currIdx)) {
                result = tuplearr[currIdx];
                currIdx = findNextIdx(currIdx+1);
            }
            else{// empty slot, find and return the next nonempty Tuple A, change currIdx to be the index of the next
                // nonempty Tuple after A
                int i = findNextIdx(currIdx+1);
                result = tuplearr[i];
                currIdx = findNextIdx(i+1);
            }
            return result;
        }

        @Override
        public void remove() {
            // it's not uncommon for a class to implement the Iterator interface
            // yet not support remove.
            throw new UnsupportedOperationException("my data can't be modified!");
        }
    }

    @Override
    public byte[] getPageData() {
         return SlottedPageFormatter.pageToBytes(this,td,pageSize);
    }

    /**
     * Fill the contents of this according to the data stored in byte array.
     * @param data
     */
    private void setPageData(byte[] data) {
         SlottedPageFormatter.bytesToPage(data,this,td);
    }

    @Override
    public Page getBeforeImage() {
        byte[] oldDataRef;
        synchronized (oldDataLock) {
            oldDataRef = Arrays.copyOf(oldData, oldData.length);
        }
        return new SlottedPage(pid, td, pageSize, oldDataRef);
    }

    @Override
    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

}
