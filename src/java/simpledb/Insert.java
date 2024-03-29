package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private DbFile table;
    private boolean done;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        this.t = t;
        this.child = child;
        this.tableid = tableid;
        this.table = Database.getCatalog().getDbFile(tableid);
        this.done = false;
        if (!table.getTupleDesc().equals(child.getTupleDesc())) {
            throw new DbException("TupleDesc mistmatch!");
        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(
                new Type[] {Type.INT_TYPE},
                new String[] {"Number_Of_Inserted_Tuples"}
            );
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.close();
        this.open();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.done) {
            return null;
        }

        int count = 0;
        try {
            while (this.child.hasNext()) {
                Tuple tuple = child.next();
                Database.getBufferPool().insertTuple(this.t, this.tableid, tuple);
                count++;
            }
        } catch (IOException e) {}

        this.done = true;
        Tuple result = new Tuple(this.getTupleDesc());
        result.setField(0, new IntField(count));
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
