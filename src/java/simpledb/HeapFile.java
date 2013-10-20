package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if(pid.getTableId() != this.getId()) {
            throw new IllegalArgumentException("Page cannot be found in this file!");
        }

        try {
            RandomAccessFile raf = new RandomAccessFile(this.f, "r");
            byte[] page = new byte[BufferPool.PAGE_SIZE];
            raf.seek(pid.pageNumber() * BufferPool.PAGE_SIZE);
            raf.read(page, 0, BufferPool.PAGE_SIZE);
            raf.close();
            return (Page) new HeapPage(new HeapPageId(pid.getTableId(), pid.pageNumber()), page);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null; // Should never execute this line.
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        RandomAccessFile raf = new RandomAccessFile(this.f, "rw");
        PageId pid = page.getId();
        long offset = BufferPool.PAGE_SIZE * pid.pageNumber();
        raf.seek(offset);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(this.f.length()/BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        ArrayList<Page> modifiedPage = new ArrayList<Page>();
        for (int i = 0; i < this.numPages(); i++) {
            HeapPageId pid = new HeapPageId(this.getId(), i);

            HeapPage p = (HeapPage) Database.getBufferPool().getPage(
                            tid, pid, Permissions.READ_WRITE
                        );

            if (p.getNumEmptySlots() != 0) {
                p.insertTuple(t);
                modifiedPage.add(p);
                return modifiedPage;
            }
        }

        HeapPageId newPid = new HeapPageId(getId(), this.numPages());
        HeapPage newP = new HeapPage(newPid, HeapPage.createEmptyPageData());
        newP.insertTuple(t);

        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.seek(this.numPages() * BufferPool.PAGE_SIZE);
        raf.write(newP.getPageData(), 0, BufferPool.PAGE_SIZE);
        raf.close();

        modifiedPage.add(newP);
        return modifiedPage;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(
                                    tid, t.getRecordId().getPageId(), 
                                    Permissions.READ_WRITE
                                );
        p.deleteTuple(t);
        return p;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

}

