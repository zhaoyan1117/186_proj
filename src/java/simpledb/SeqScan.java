package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private int tableid;
    private String tableAlias;

    private DbFileIterator i;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        if (tid == null) {
            tid = new TransactionId();
        }
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias() {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.i = Database.getCatalog().getDbFile(tableid).iterator(this.tid);
        this.i.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
        
        String[] fieldAr = new String[td.numFields()];
        Type[] typeAr = new Type[td.numFields()];
        
        for (int i = 0; i < td.numFields(); i++) {
            typeAr[i] = td.getFieldType(i);
            fieldAr[i] = strOrNull(tableAlias) + "." + strOrNull(td.getFieldName(i));
        }

        return new TupleDesc(typeAr, fieldAr);
    }

    private String strOrNull(String s) {
        if (s == null) {
            return "null";
        } else {
            return s;
        }
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.i == null) {
            throw new IllegalStateException("Iterator has not been opened yet!");
        } else {
            return this.i.hasNext();
        }
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (this.i == null) {
            throw new IllegalStateException("Iterator has not been opened yet!");
        }
        
        return this.i.next();
    }

    public void close() {
        // some code goes here
        this.i = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        if (this.i == null) {
            throw new IllegalStateException("Iterator has not been opened yet!");
        }

        this.i.rewind();
    }
}
