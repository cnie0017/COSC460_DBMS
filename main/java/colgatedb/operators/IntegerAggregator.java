package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.*;
import java.util.*;

import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are taken almost verbatim from the SimpleDB project.
 * We are grateful for Sam's permission to use and adapt his materials.
 */

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private HashMap<Field,AggregateFields> aggrMap;
    // stores groupby field value and its AggregateFields object, which contains info such as max and min

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        op = what;
        aggrMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field tupfield;
        if (gbfield == Aggregator.NO_GROUPING){
            tupfield = null;
        }
        else{
            tupfield =  tup.getField(gbfield);
        }
        if (!aggrMap.containsKey(tupfield)){
            //create an AggregateFields object for this groupby value
            String s = tupfield.toString();
            aggrMap.put(tupfield,new AggregateFields(s));
        }
        IntField tupleaggrfield = (IntField)tup.getField(afield);
        int tuplevalue = tupleaggrfield.getValue();
        AggregateFields aggrfield = aggrMap.get(tupfield);
        //min
        if (tuplevalue < aggrfield.min) {
            aggrfield.min =tuplevalue;
        }
        //max
        if (tuplevalue >= aggrfield.max) {
            aggrfield.max = tuplevalue;
        }
        //sum
        aggrfield.sum += tuplevalue;
        //count
        aggrfield.count++;
        //average
        aggrfield.sumCount = aggrfield.sum/aggrfield.count;
        aggrMap.put(tupfield,aggrfield);
    }

    /**
     * Generates tupledesc for the result tuples
     */
    private TupleDesc generateTupleDesc() {
        String[] namearr;
        Type[] typearr;
        if (gbfield != Aggregator.NO_GROUPING) {
            namearr = new String[] {"groupValue", "aggregateValue"};
            typearr = new Type[] {gbfieldtype, Type.INT_TYPE};
        }
        else {
            namearr = new String[] {"aggregateValue"};
            typearr = new Type[] {Type.INT_TYPE};
        }
        return new TupleDesc(typearr, namearr);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tuplelist = new ArrayList<Tuple>();
        TupleDesc td = generateTupleDesc();
        Tuple newtuple;
        for(Field groupby : aggrMap.keySet()) {
            AggregateFields af = aggrMap.get(groupby);
            newtuple = new Tuple(td);
            int result = 0;
            if (op.equals(Op.SUM)){
                result = af.sum;
            }
            else if (op.equals(Op.AVG)){
                result = af.sumCount;
            }
            else if (op.equals(Op.MIN)){
                result = af.min;
            }
            else if (op.equals(Op.MAX)){
                result = af.max;
            }
            if (gbfield == Aggregator.NO_GROUPING){
                newtuple.setField(0, new IntField(result));
            }
            else {
                newtuple.setField(0, groupby);
                newtuple.setField(1, new IntField(result));
            }
            tuplelist.add(newtuple);
        }
        return new TupleIterator(td, tuplelist);
    }

    /**
     * A helper struct to store accumulated aggregate values.
     */
    private class AggregateFields {
        public String groupVal;
        public int min, max, sum, count, sumCount;//sumCount = average?

        public AggregateFields(String groupVal) {
            this.groupVal = groupVal;
            min = Integer.MAX_VALUE;
            max = Integer.MIN_VALUE;
            sum = count = sumCount = 0;
        }
    }

}
