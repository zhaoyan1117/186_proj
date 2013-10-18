package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
    
    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private HeapFile file;

    private int currentPageNumber;
    private Iterator<Tuple> currentPageIterator;
    private BufferPool buffer;
    private int numPages;

    public HeapFileIterator(TransactionId tid, HeapFile file) {
        this.tid = tid;
        this.file = file;
        this.buffer = Database.getBufferPool();
        this.numPages = this.file.numPages();
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.currentPageNumber = 0;
        this.setCurrentPageIterator();
    }

    private void setCurrentPageIterator() throws TransactionAbortedException, DbException {
        HeapPageId currentPageId = new HeapPageId(this.file.getId(), this.currentPageNumber);
        HeapPage currentPage = (HeapPage) this.buffer.getPage(this.tid, currentPageId, Permissions.READ_WRITE);
        this.currentPageIterator = currentPage.iterator();
    }

    /** @return true if there are more tuples available. */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (this.currentPageIterator == null) {
            return false; // return false if iterator is close.
        } else if (this.currentPageIterator.hasNext()) {
            return true;
        } else if (this.currentPageNumber < this.numPages - 1) {
            this.currentPageNumber++;
            this.setCurrentPageIterator();
            return this.hasNext();
        } else {
            return false;
        }
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (this.hasNext()) {
            return this.currentPageIterator.next();
        } else {
            throw new NoSuchElementException("It is the end of the iterator!");
        }
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.currentPageNumber = 0;
        this.setCurrentPageIterator();
    }

    /**
     * Closes the iterator.
     */
    @Override
    public void close() {
        this.currentPageIterator = null;
    }
}