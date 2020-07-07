package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.Type;

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
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {
    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private Type afieldtype;
    private Type gfieldtype;
    private DbIterator it;

    /**
     * Constructor.
     * <p/>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        gfieldtype = child.getTupleDesc().getFieldType(gfield);
        afieldtype = child.getTupleDesc().getFieldType(afield);
        if (afieldtype.equals(Type.INT_TYPE)){
            aggregator = new IntegerAggregator(gfield,gfieldtype,afield,aop);
        }
        else{
            aggregator = new StringAggregator(gfield,gfieldtype,afield,aop);
        }

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        if (gfield == -1){
            return Aggregator.NO_GROUPING;
        }
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples If not, return
     * null;
     */
    public String groupFieldName() {
        if (gfield == -1){
            return null;
        }
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();
        while (child.hasNext()) {
            Tuple t = child.next();
            aggregator.mergeTupleIntoGroup(t);
        }
        it = aggregator.iterator();
        it.open();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return it.hasNext();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (!hasNext()){
            throw new NoSuchElementException("");
        }
        return it.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p/>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        String[] namearr;
        Type[] typearr;
        if (gfield != Aggregator.NO_GROUPING) {
            namearr = new String[] {"groupValue", nameOfAggregatorOp(aop)+aggregateFieldName()};
            typearr = new Type[] {gfieldtype, Type.INT_TYPE};
        }
        else {
            namearr = new String[] {nameOfAggregatorOp(aop)+aggregateFieldName()};
            typearr = new Type[] {Type.INT_TYPE};
        }
        return new TupleDesc(typearr, namearr);
    }

    public void close() {
        child.close();
        aggregator.iterator().close();
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 2) {
            throw new DbException("Expected only one child!");
        }
        child = children[0];;
    }

}
