package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private Map<String, Integer> minMap;
    private Map<String, Integer> maxMap;
    private Map<String, IntHistogram> intHistograms;
    private Map<String, StringHistogram> stringHistograms;
    private int ntups;

    private TupleDesc fielTd;
    private DbFileIterator fileItr;
    private int fileNumPages;

    private int ioCostPerPage;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        HeapFile file = (HeapFile) Database.getCatalog().getDbFile(tableid);
        this.fielTd = file.getTupleDesc();
        this.fileItr = file.iterator(null);
        this.fileNumPages = file.numPages();
        minMap = new HashMap<String, Integer>();
        maxMap = new HashMap<String, Integer>();
        intHistograms = new HashMap<String, IntHistogram>();
        stringHistograms = new HashMap<String, StringHistogram>();
        this.ioCostPerPage = ioCostPerPage;

        getStatistics();
        buildHistograms();
    }

    private void getStatistics() {
        try {
            this.fileItr.open();
            while(this.fileItr.hasNext()) {
                Tuple t = this.fileItr.next();
                this.ntups++;
                for (int i = 0; i < this.fielTd.numFields(); i++) {
                    if (this.fielTd.getFieldType(i).equals(Type.INT_TYPE)) {
                        IntField field = (IntField) t.getField(i);
                        String name = this.fielTd.getFieldName(i);

                        if (!this.minMap.containsKey(name)) {
                            this.minMap.put(name, field.getValue());
                        } else {
                            this.minMap.put(name, Math.min(field.getValue(), this.minMap.get(name)));
                        }

                        if (!this.maxMap.containsKey(name)) {
                            this.maxMap.put(name, field.getValue());
                        } else {
                            this.maxMap.put(name, Math.max(field.getValue(), this.maxMap.get(name)));
                        }
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        this.fileItr.close();
    }

    private void buildHistograms() {
        try {
            this.fileItr.open();
            while (this.fileItr.hasNext()) {
                Tuple t = this.fileItr.next();

                for (int i = 0; i < this.fielTd.numFields(); i++) {
                    String name = this.fielTd.getFieldName(i);

                    if (this.fielTd.getFieldType(i).equals(Type.INT_TYPE)) {

                        IntField field = (IntField)t.getField(i);
                        IntHistogram h = intHistograms.get(name);
                        if (h == null) { h = new IntHistogram(NUM_HIST_BINS, 
                                                              minMap.get(name),
                                                              maxMap.get(name)); }
                        h.addValue(field.getValue());
                        intHistograms.put(name, h);
                    } else {
                        StringField field = (StringField)t.getField(i);
                        StringHistogram h = stringHistograms.get(name);
                        if (h == null) { h = new StringHistogram(NUM_HIST_BINS); }
                        h.addValue(field.getValue());
                        stringHistograms.put(name, h);
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }

        this.fileItr.close();
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.fileNumPages * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (this.ntups * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        String name = this.fielTd.getFieldName(field);

        if (this.fielTd.getFieldType(field).equals(Type.INT_TYPE)) {
            return this.intHistograms.get(name).estimateSelectivity(op, ((IntField)constant).getValue());
        } else {
            return this.stringHistograms.get(name).estimateSelectivity(op, ((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.ntups;
    }

}
