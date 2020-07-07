package colgatedb.main;

import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.operators.*;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.Op;
import colgatedb.tuple.StringField;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.Type;

import java.io.IOException;
import java.util.ArrayList;

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
public class OperatorMain {

    public static void main(String[] argv)
            throws DbException, TransactionAbortedException, IOException {

        // file named college.schema must be in colgatedb directory
        String filename = "college.schema";
        System.out.println("Loading schema from file: " + filename);
        Database.getCatalog().loadSchema(filename);

        // SQL query: SELECT * FROM STUDENTS WHERE name="Alice"
        // algebra translation: select_{name="alice"}( Students )
        // query plan: a tree with the following structure
        // - a Filter operator is the root; filter keeps only those w/ name=Alice
        // - a SeqScan operator on Students at the child of root
        /*TransactionId tid = new TransactionId();
        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("Students"));
        StringField alice = new StringField("alice", Type.STRING_LEN);
        Predicate p = new Predicate(1, Op.EQUALS, alice);
        DbIterator filterStudents = new Filter(p, scanStudents);

        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        filterStudents.open();
        while (filterStudents.hasNext()) {
            Tuple tup = filterStudents.next();
            System.out.println("\t"+tup);
        }
        filterStudents.close();*/

        //Task 6
        TransactionId tid2 = new TransactionId();
        //filter professor hay from Profs
        DbIterator scanProfs = new SeqScan(tid2,Database.getCatalog().getTableId("Profs"));
        StringField hay = new StringField("hay",Type.STRING_LEN);
        Predicate p_on_hay = new Predicate(1,Op.EQUALS,hay);
        DbIterator filterProfname = new Filter(p_on_hay, scanProfs);
        //join T & filterProfname, on hay's fav course, should have 2 tuples
        DbIterator scanTakes = new SeqScan(tid2,Database.getCatalog().getTableId("Takes"));
        JoinPredicate favCourse = new JoinPredicate(1,Op.EQUALS,2);
        Join joinTP = new Join(favCourse,scanTakes,filterProfname);
        //Join students & TP
        SeqScan scanStudents = new SeqScan(tid2, Database.getCatalog().getTableId("Students"));
        JoinPredicate studensTaking = new JoinPredicate(0,Op.EQUALS,0);
        Join joinwithS = new Join(studensTaking,scanStudents,joinTP);

        //project on name
        ArrayList<Integer> idxarr = new ArrayList<>();
        Type[] tyarr = new Type[]{Type.STRING_TYPE};
        idxarr.add(scanStudents.getTupleDesc().fieldNameToIndex("Studentsname"));
        Project result = new Project(idxarr,tyarr,joinwithS);
        System.out.println("Projection results:");
        result.open();
        while (result.hasNext()) {
            Tuple t = result.next();
            System.out.println("\t"+t);
        }
        result.close();




    }

}