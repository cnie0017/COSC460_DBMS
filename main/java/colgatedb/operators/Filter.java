package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

import java.util.ArrayList;
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
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private boolean open;
    private DbIterator child;
    private TupleDesc td;
    private ArrayList<Integer> selectedFieldIds;
    private Predicate pred;
    private Tuple current;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        pred = p;
        this.child = child;
        open = false;
        td = child.getTupleDesc();
        current = null;
    }

    public Predicate getPredicate() {
        return pred;
    }
    public TupleDesc getTupleDesc(){
        return td;
    }


    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        open = true;
    }

    @Override
    public void close() {
        child.close();
        open = false;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    @Override
    // should do heavy lifting
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (open){
            while( child.hasNext()){
                Tuple t = child.next();
                if (pred.filter(t)) {
                    current = t;
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    //should be super simple, call hasNext()
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (!open){
            throw new NoSuchElementException("Closed!");
        }
        if (current == null){
            if ( !hasNext()) {
                throw new NoSuchElementException();
            }
        }
        Tuple temp = current;
        current = null;
        return temp;
    }

    @Override
    public DbIterator[] getChildren() { return new DbIterator[]{this.child}; }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 1) {
            throw new DbException("Expected only one child!");
        }
        child = children[0];
    }

}
