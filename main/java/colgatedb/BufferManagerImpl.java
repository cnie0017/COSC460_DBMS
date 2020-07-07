package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;
import colgatedb.page.SimplePageId;


import java.util.*;
import java.util.HashMap;


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
public class BufferManagerImpl implements BufferManager {

    private boolean allowEvictDirty = false;  // a flag indicating whether a dirty page is candidate for eviction
    private HashMap<PageId, Frame> pool;
    private int numpage;
    private DiskManager dm;
    private Frame start;// the starting frame in the doubly linked list for implementing LRU
    private Frame tail;

    /**
     * Construct a new buffer manager.
     * @param numPages maximum size of the buffer pool
     * @param dm the disk managerr to call to read/write pages
     */
    public BufferManagerImpl(int numPages, DiskManager dm) {
        numpage = numPages;
        this.dm = dm;
        pool = new HashMap<PageId, Frame>();
        start = null;
        tail = null;
    }


    @Override
    public synchronized Page pinPage(PageId pid, PageMaker pageMaker) {

        if (pool.containsKey(pid)) {// already exists in pool
            Frame f = pool.get(pid);
            f.pinCount++;
            removeFrame(f);//take this frame out from the linked list
            setToStart(f);// set this frame to be the start of linked list since it is recently used
            return f.page;
        }
        int oldsize = pool.size();
        if (oldsize >= numpage){
            //bigger than size, need to evict for making space
            evictPage();
            if (pool.size() == oldsize) {
                throw new BufferManagerException("Cannot evict!");
            }
        }
        //otherwise, get page from disk
        Page newpage = dm.readPage(pid, pageMaker);
        Frame newframe = new Frame(newpage);
        setToStart(newframe);// since it is recently used
        pool.put(pid, newframe);
        return newpage;
    }

    @Override
    public synchronized void unpinPage(PageId pid, boolean isDirty) {
        if (pool.containsKey(pid)){
            Frame f = pool.get(pid);
            if (f.pinCount >= 1){
                f.pinCount--;
                //check if data has been modified, if yes, even if its isDirty is false,
                // it should be marked as True and considered as a candidate for evicting
                if ((!f.isDirty) || (isDirty)){
                    f.isDirty = isDirty;
                }
                removeFrame(f);// since it is recently used
                setToStart(f);
                return;
            }
        }
        throw new BufferManagerException("Cannot unpinPage!");
    }

    @Override
    public synchronized void flushPage(PageId pid) {
        //only flush a page that is dirty, does not remove the page from the pool
        Frame f = pool.get(pid);
        if (f.isDirty){
            dm.writePage(f.page);
            f.isDirty = false;
        }
    }

    @Override
    public synchronized void flushAllPages() {
        for (Frame frame: pool.values()){
            if (frame.isDirty){
                dm.writePage(frame.page);
                frame.isDirty = false;
            }
        }
    }

    @Override
    public synchronized void evictDirty(boolean allowEvictDirty) {
    // revised version: Only sets this.allowEvictDirty
       this.allowEvictDirty = allowEvictDirty;
    }

    //helper method that set aframe to the start of linked list since it is recently used
    public synchronized void setToStart(Frame aframe) {
        //Set aframe to be the new start
        aframe.next = start;
        aframe.last = null;
        if (start != null) {
            start.last = aframe;
        }
        start = aframe;
        if (tail == null){
            tail = start;
        }
    }


    //helper function that removes aframe from its current position and adjusts the last and next of aframe
    public synchronized void removeFrame(Frame aframe) {
        if (aframe.last == null) {//aframe is the first frame in pool
            start = aframe.next;
        } else {
            aframe.last.next = aframe.next;
        }
        if (aframe.next == null) {//aframe is the last frame
            tail = aframe.last;
        } else {
            aframe.next.last = aframe.last;
        }

    }


    /**
     * uses a LRU replacement policy to evict page. Implemented a doubly linked list of frames, each with a
     * last and tail field, points to the previous frame and next frame respectively. Two private fields, start
     * and tail are also added, to indicate the first and last frame in the list. A recently used page is
     * put at the start of this list and the tail is always the least recently used page and thus should be considered
     * as potential target to remove
     */
    private synchronized void evictPage(){
        Frame temp = tail;
        SimplePageId pid = (SimplePageId) temp.page.getId();
        while(temp != null){
            //can remove
            if (temp.pinCount == 0 && (!temp.isDirty || (temp.isDirty && this.allowEvictDirty))){
                flushPage(pid);
                removeFrame(temp);
                pool.remove(pid);
                return;
            }
            else{
                if (temp.last!=null) {
                    temp = temp.last;
                    pid = (SimplePageId) temp.page.getId();
                }
                else {
                    return;
                }
            }
        }
    }



    @Override
    public synchronized void allocatePage(PageId pid) {
        dm.allocatePage(pid);
    }

    @Override
    public synchronized boolean isDirty(PageId pid) {
        if (inBufferPool(pid)) {
            return pool.get(pid).isDirty;
        }
        return false;
    }

    @Override
    public synchronized boolean inBufferPool(PageId pid) {
        return pool.containsKey(pid);
    }

    @Override
    public synchronized Page getPage(PageId pid) {
        if (inBufferPool(pid)){
            return pool.get(pid).page;
        }
        throw new BufferManagerException("Page not in BufferPool!");
    }

    @Override
    public synchronized void discardPage(PageId pid) {
        // remove any existent page from the pool but NOT flush to disk
        if (inBufferPool(pid)){
            pool.remove(pid);
        }
    }


    /**
     * A frame holds one page and maintains state about that page.  You are encouraged to use this
     * in your design of a BufferManager.  You may also make any warranted modifications.
     */
    private class Frame {
        private Page page;
        private int pinCount;
        public boolean isDirty;
        public Frame last;
        public Frame next;

        public Frame(Page page) {
            this.page = page;
            this.pinCount = 1;   // assumes Frame is created on first pin -- feel free to modify as you see fit
            this.isDirty = false;
            this.last = null;
            this.next = null;
        }
    }

}