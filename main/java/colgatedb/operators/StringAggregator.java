package colgatedb.operators;

import colgatedb.tuple.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private HashMap<Field,AggregateFields> aggrMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        op = what;
        aggrMap = new HashMap<>();
        if (what != Op.COUNT){
            throw new IllegalArgumentException("");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field tupfield;
        if (gbfield == Aggregator.NO_GROUPING){
            tupfield = null;
        }
        else{
            tupfield = tup.getField(gbfield);
        }
        if (!aggrMap.containsKey(tupfield)){
            //create an AggregateFields object for this groupby value
            String s = tupfield.toString();
            aggrMap.put(tupfield,new AggregateFields(s));
        }
        //IntField tupleaggrfield = (IntField)tup.getField(afield);
        //int tuplevalue = tupleaggrfield.getValue();
        AggregateFields aggrfield = aggrMap.get(tupfield);
        aggrfield.count++;
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
     * @return a DbIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tuplelist = new ArrayList<Tuple>();
        TupleDesc td = generateTupleDesc();
        Tuple newtuple;
        for(Field groupby : aggrMap.keySet()) {
            AggregateFields af = aggrMap.get(groupby);
            newtuple = new Tuple(td);
            if (gbfield == Aggregator.NO_GROUPING){
                newtuple.setField(0, new IntField(af.count));
            }
            else {
                newtuple.setField(0, groupby);
                newtuple.setField(1, new IntField(af.count));
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
        public int count;

        public AggregateFields(String groupVal) {
            this.groupVal = groupVal;
            count = 0;
        }
    }
}
