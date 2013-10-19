package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private String afieldName; // Used when build TupleDesc.
    private String gbfieldName; // Used when build TupleDesc.

    private Map<Field, Field> aggregates;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(!what.equals(Op.COUNT)) 
            throw new IllegalArgumentException("StringAggregator must have COUNT Operator!");

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;

        aggregates = new HashMap<Field, Field>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        afieldName = tup.getTupleDesc().getFieldName(afield);

        Field key;
        if (gbfield == NO_GROUPING) {
            key = new IntField(NO_GROUPING);
        } else {
            key = tup.getField(gbfield);
            gbfieldName = tup.getTupleDesc().getFieldName(gbfield);
        }

        IntField value = (IntField) aggregates.get(key);

        int count;

        if (value == null) {
            count = 1;
        } else {
            count = value.getValue() + 1;
        }

        aggregates.put(key, new IntField(count));
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        List<Tuple> tupleList = new ArrayList<Tuple>();
        TupleDesc td = buildTupleDesc();

        if (this.gbfield == NO_GROUPING) {
            Tuple aggregateTuple = new Tuple(td);
            aggregateTuple.setField(0, aggregates.get(new IntField(NO_GROUPING)));

            tupleList.add(aggregateTuple);
        } else {
            for (Map.Entry<Field, Field> agg : aggregates.entrySet()) { 
                Tuple aggregateTuple = new Tuple(td);
                aggregateTuple.setField(0, agg.getKey());
                aggregateTuple.setField(1, agg.getValue());

                tupleList.add(aggregateTuple);
            }
        }

        return new TupleIterator(td, tupleList);
    }

    private TupleDesc buildTupleDesc() {
        Type[] typeAr; 
        String[] fieldAr;

        if (this.gbfield == NO_GROUPING) {
            typeAr = new Type[] { Type.INT_TYPE };
            fieldAr = new String[] { this.afieldName };
        } else {
            typeAr = new Type[] { this.gbfieldtype, Type.INT_TYPE };
            fieldAr = new String[] { this.gbfieldName, this.afieldName };
        }

        return new TupleDesc(typeAr, fieldAr);
    }

}
