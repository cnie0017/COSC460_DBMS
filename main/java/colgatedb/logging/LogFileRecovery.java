package colgatedb.logging;

import colgatedb.Database;
import colgatedb.DiskManager;
import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.transactions.TransactionId;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Set;

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
public class LogFileRecovery {

    private final RandomAccessFile readOnlyLog;

    /**
     * Helper class for LogFile during rollback and recovery.
     * This class given a read only view of the actual log file.
     *
     * If this class wants to modify the log, it should do something
     * like this:  Database.getLogFile().logAbort(tid);
     *
     * @param readOnlyLog a read only copy of the log file
     */
    public LogFileRecovery(RandomAccessFile readOnlyLog) {
        this.readOnlyLog = readOnlyLog;
    }

    /**
     * Print out a human readable representation of the log
     */
    public void print() throws IOException {
        System.out.println("-------------- PRINT OF LOG FILE -------------- ");
        // since we don't know when print will be called, we can save our current location in the file
        // and then jump back to it after printing
        Long currentOffset = readOnlyLog.getFilePointer();

        readOnlyLog.seek(0);
        long lastCheckpoint = readOnlyLog.readLong(); // ignore this
        System.out.println("BEGIN LOG FILE");
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    System.out.println("<T_" + tid + " BEGIN>");
                    break;
                case LogType.COMMIT_RECORD:
                    System.out.println("<T_" + tid + " COMMIT>");
                    break;
                case LogType.ABORT_RECORD:
                    System.out.println("<T_" + tid + " ABORT>");
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFileImpl.readPageData(readOnlyLog);
                    Page afterImg = LogFileImpl.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " UPDATE pid=" + beforeImg.getId() +">");
                    break;
                case LogType.CLR_RECORD:
                    afterImg = LogFileImpl.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " CLR pid=" + afterImg.getId() +">");
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    Set<Long> tids = new HashSet<Long>();
                    for (int i = 0; i < count; i++) {
                        long nextTid = readOnlyLog.readLong();
                        tids.add(nextTid);
                    }
                    System.out.println("<T_" + tid + " CHECKPOINT " + tids + ">");
                    break;
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            long startOfRecord = readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
        }
        System.out.println("END LOG FILE");

        // return the file pointer to its original position
        readOnlyLog.seek(currentOffset);

    }

    /** undo
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     *
     * @param tidToRollback The transaction to rollback
     * @throws java.io.IOException if tidToRollback has already committed
     */
    public void rollback(TransactionId tidToRollback) throws IOException {
        // move to the end of file to read the start location of last record
        readOnlyLog.seek(readOnlyLog.length()-LogFileImpl.LONG_SIZE);

        while (readOnlyLog.getFilePointer() > 0) {
            long startOfRecord = readOnlyLog.readLong();
            readOnlyLog.seek(startOfRecord);

            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();

            if (tidToRollback.getId()==tid){
                switch (type) {
                    case LogType.COMMIT_RECORD:
                        throw new IOException();
                    case LogType.UPDATE_RECORD:
                        Page beforeImg = LogFileImpl.readPageData(readOnlyLog);
                        Page afterImg = LogFileImpl.readPageData(readOnlyLog);
                        Database.getDiskManager().writePage(beforeImg);
                        Database.getBufferManager().discardPage(beforeImg.getId());//discard current
                        Database.getLogFile().logAbort(tidToRollback.getId());
                        Database.getLogFile().logCLR(tid, beforeImg);//wirte CLR

                }
            }
            readOnlyLog.seek(startOfRecord-LogFileImpl.LONG_SIZE);
        }
    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     */
    public void recover() throws IOException {
        HashSet<Long> losers = new HashSet<Long>();
        // start from last checkpoint
        readOnlyLog.seek(0);
        long lastCheckpoint = readOnlyLog.readLong();

        if (lastCheckpoint != -1) {
            readOnlyLog.seek(lastCheckpoint);
            readOnlyLog.seek(readOnlyLog.getFilePointer()+LogFileImpl.INT_SIZE+LogFileImpl.LONG_SIZE);
            int totalloser = readOnlyLog.readInt();
            for (int i = 0; i < totalloser; i++){
                losers.add(readOnlyLog.readLong());
            }
            // no check point
            readOnlyLog.seek(readOnlyLog.getFilePointer() +
                    LogFileImpl.LONG_SIZE);
        }
        //redoes non-loser transactions
        redo(losers);
        //undo updates for losers
        undo(losers);
    }

    private void redo(HashSet<Long> losers) throws IOException {
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()){
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    if (losers.contains(tid)) { throw new IOException(); }  //already begun
                    losers.add(tid);
                    break;
                case LogType.COMMIT_RECORD:
                    if (!losers.contains(tid)){throw new IOException();}
                    //already committed, can't commit again
                    losers.remove(tid);
                    break;
                case LogType.ABORT_RECORD:
                    if (!losers.contains(tid)) {throw new IOException();}
                    // already aborted, can't commit again
                    losers.remove(tid);
                    break;
                case LogType.UPDATE_RECORD:
                    if (!losers.contains(tid)) {throw new IOException();}
                    Page beforeImg = LogFileImpl.readPageData(readOnlyLog);
                    Page afterImg = LogFileImpl.readPageData(readOnlyLog);
                    Database.getBufferManager().discardPage(beforeImg.getId());
                    Database.getDiskManager().writePage(afterImg);
                    break;
                case LogType.CLR_RECORD:
                    if (!losers.contains(tid)){throw new IOException();}
                    afterImg = LogFileImpl.readPageData(readOnlyLog);
                    Database.getDiskManager().writePage(afterImg);
                    break;
                case LogType.CHECKPOINT_RECORD:
                    throw new RuntimeException("");//Should not encounter checkpoint
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            // go to next log record
            readOnlyLog.seek(readOnlyLog.getFilePointer() + LogFileImpl.LONG_SIZE);
        }
    }

    //undo updates of losers transactions
    private void undo(HashSet<Long> losers) throws IOException {
        // move to the end of file
        readOnlyLog.seek(readOnlyLog.length());
        long currentoffset = readOnlyLog.getFilePointer();
        while (currentoffset > LogFileImpl.LONG_SIZE && !losers.isEmpty()) {
            readOnlyLog.seek(currentoffset - LogFileImpl.LONG_SIZE);
            long start = readOnlyLog.readLong();
            readOnlyLog.seek(start);

            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();

            switch (type) {
                case LogType.BEGIN_RECORD:
                    if (losers.contains(tid)) {
                        Database.getLogFile().logAbort(tid);
                        losers.remove(tid);
                    }
                    break;
                case LogType.UPDATE_RECORD:
                    if (losers.contains(tid)) {
                        Page beforeImg = LogFileImpl.readPageData(readOnlyLog);
                        Page afterImg = LogFileImpl.readPageData(readOnlyLog);
                        Database.getBufferManager().discardPage(beforeImg.getId());
                        Database.getDiskManager().writePage(beforeImg);
                        Database.getLogFile().logCLR(tid, beforeImg);
                    }
                    break;
            }
            currentoffset = start;
        }
    }
}
