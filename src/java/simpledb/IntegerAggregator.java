package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private String afieldName; // Used when build TupleDesc.
    private String gbfieldName; // Used when build TupleDesc.

    private Map<Field, Field> aggregates;
    private Map<Field, Integer> countMap;
    private Map<Field, Integer> sumMap;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregates = new HashMap<Field, Field>();
        if (what == Op.AVG) {
            this.countMap = new HashMap<Field, Integer>();
            this.sumMap = new HashMap<Field, Integer>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        this.afieldName = tup.getTupleDesc().getFieldName(afield);
        Field key;
        if (this.gbfield == Aggregator.NO_GROUPING) {
            key = new IntField(Aggregator.NO_GROUPING);
        } else {
            this.gbfieldName = tup.getTupleDesc().getFieldName(gbfield);
            key = tup.getField(gbfield);
        }
    
        IntField value = (IntField) aggregates.get(key);
        IntField currentAggValue = (IntField) tup.getField(afield);

        switch (what) {
            case COUNT: this.handleCount(key, value, currentAggValue); break;
            case SUM: this.handleSUM(key, value, currentAggValue); break;
            case AVG: this.handleAVG(key, value, currentAggValue); break;
            case MIN: this.handleMIN(key, value, currentAggValue); break;
            case MAX: this.handleMAX(key, value, currentAggValue); break;
            default: 
                throw new IllegalStateException("Wrong Op type in IntegerAggregator!"); 
        }
    }

    private void handleCount (Field key, IntField value, IntField currentAggValue) {
        int count;
        if (value == null) {
            count = 1;
        } else {
            count = value.getValue()+1;
        }

        aggregates.put(key, new IntField(count));
    }    

    private void handleSUM (Field key, IntField value, IntField currentAggValue) {
        int sum;

        if (value == null) {
            sum = currentAggValue.getValue();
        } else {
            sum = value.getValue() + currentAggValue.getValue();
        }

        aggregates.put(key, new IntField(sum));
    }

    private void handleAVG (Field key, IntField value, IntField currentAggValue) {
        int count;
        int sum;
        int avg;

        if (value == null) {
            avg = currentAggValue.getValue();
            count = 1;
            sum = avg;
        } else {
            sum = sumMap.get(key) + currentAggValue.getValue();
            count = countMap.get(key) + 1;
            avg = sum / count;
        }

        sumMap.put(key, sum);
        countMap.put(key, count);
        aggregates.put(key, new IntField(avg));
    }

    private void handleMIN (Field key, IntField value, IntField currentAggValue) {
        int min;

        if (value == null) {
            min = currentAggValue.getValue();
        } else {
            min = Math.min(currentAggValue.getValue(), value.getValue());
        }

        aggregates.put(key, new IntField(min));
    }

    private void handleMAX (Field key, IntField value, IntField currentAggValue) {
        int max;

        if (value == null) {
            max = currentAggValue.getValue();
        } else {
            max = Math.max(currentAggValue.getValue(), value.getValue());
        }

        aggregates.put(key, new IntField(max));
    }    

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
