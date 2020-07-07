package colgatedb.tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import java.util.HashMap;
import java.util.Map;


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
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc mytd;
    private HashMap<Integer,Field> map;
    private RecordId rid;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        mytd = td;
        map = new HashMap<>();
    }

    /*
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return mytd;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     * @throws RuntimeException if f does not match type of field i.
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public void setField(int i, Field f) {
        if (i >= 0 && i < mytd.numFields()) {
            if (!mytd.getFieldType(i).equals(f.getType())) {
                throw new RuntimeException("Field type does not match!");
            }
            else{
                map.put(i,f);
            }
        }
        else{
            throw new NoSuchElementException("Invalid field reference i!");
        }
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Field getField(int i) {
        if (i >= 0 && i < mytd.numFields()){
            return map.get(i);
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is a tab and \n is a newline
     */
    public String toString() {
        String result = "";
        for (int i = 0; i < map.size()-1; i++){
            result += map.get(i)+"\t".toString();
        }
        result += map.get(map.size()-1).toString();
        return result;
    }


    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // hint: use java.util.Arrays.asList to convert array into a list, then return list iterator.
        ArrayList<Field> list1 = new ArrayList<Field>(map.values());
        Iterator<Field> FieldsIterator = list1.iterator();
        return FieldsIterator;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May be null.
     */
    public RecordId getRecordId() {
        if (rid == null || rid.getPageId() == null){
            return null;
        }
         return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }
}
