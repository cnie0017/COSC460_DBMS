package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.dbfile.DbFileIterator;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.Field;
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
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {
    private DbIterator child1;
    private DbIterator child2;
    private JoinPredicate joinp;
    private boolean open;
    private TupleDesc td1;
    private TupleDesc td2;
    private TupleDesc newtd;
    private Tuple joinresult;
    private Tuple t1;
    private Tuple t2;


    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.child1 = child1;
        this.child2 = child2;
        this.joinp = p;
        open = false;
        joinresult = null;
        td1 = child1.getTupleDesc();
        td2 = child2.getTupleDesc();
        newtd = TupleDesc.merge(td1,td2);
        t1 = null;
        t2 = null;
    }

    public JoinPredicate getJoinPredicate() {
        return joinp;
    }

    public TupleDesc getTupleDesc(){return newtd;}

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child1.open();
        child2.open();
        open = true;
    }

    @Override
    public void close() {
        child1.close();
        child2.close();
        open = false;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
        t1 = null;
        t2 = null;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        // if there exists a non-null result that has not been returned by Next(), return true
        if (joinresult!=null){
            return true;
        }
        while (t1!=null ||child1.hasNext()){
            if (t1 == null) {
                t1 = child1.next();
            }
            while (child2.hasNext()){
                t2 = child2.next();
                if (joinp.filter(t1,t2)){
                    int i = 0;
                    Tuple newt = new Tuple(newtd);
                    Iterator<Field> t1f= t1.fields();
                    while (t1f.hasNext()){
                        newt.setField(i,t1f.next());
                        i++;
                    }
                    Iterator<Field> t2f= t2.fields();
                    while (t2f.hasNext()){
                        newt.setField(i,t2f.next());
                        i++;
                    }
                    joinresult = newt;
                    return true;
                }
            }
            t1 = null;
            child2.rewind();
        }
        return false;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. The implementation is a simple nested loops join.
     * <p/>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p/>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!open){
            throw new NoSuchElementException("Closed!");
        }
        if (joinresult == null){
            if ( !hasNext()) {
                throw new NoSuchElementException();
            }
        }
        Tuple temp = joinresult;
        joinresult = null;
        return temp;

    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child1,child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 2) {
            throw new DbException("Expected only one child!");
        }
        child1 = children[0];
        child2 = children[1];
    }

}