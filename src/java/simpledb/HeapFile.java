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


    private final File dbFile;
    private final TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.dbFile = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return dbFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return dbFile.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableid = pid.getTableId();
        int pgNo = pid.getPageNumber();

        RandomAccessFile f = null;

        try{
            f = new RandomAccessFile(dbFile, "r");
            if((pgNo+1)*BufferPool.getPageSize() > f.length()) {
                f.close();
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableid, pgNo));
            }

            byte[] bytes = HeapPage.createEmptyPageData();
            f.seek(pgNo * BufferPool.getPageSize());
            int read = f.read(bytes,0,BufferPool.getPageSize());
            if(read < 0) {
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableid, pgNo, read));
            }
            return new HeapPage(new HeapPageId(tableid, pgNo), bytes);
        }catch (IOException e){
            throw new IllegalArgumentException("HeapFile: readPage: file not found");
        }finally {
            try{
                f.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    //为什么是向下取整
    public int numPages() {
        int num = (int)Math.floor(dbFile.length()*1.0/BufferPool.getPageSize());
        return num;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator{
        private int curPg;
        private Iterator<Tuple> tupleIter;
        private final TransactionId tid;
        private final int tableId;
        private final int numPages;

        public HeapFileIterator(TransactionId tid){
            this.tid = tid;
            this.tableId = getId();    //内部类可以直接访问外部类的方法
            this.numPages = numPages();
            tupleIter = null;
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            curPg = 0;
            tupleIter = getPageTuples(curPg);
        }

        private Iterator<Tuple> getPageTuples(int pgNum) throws TransactionAbortedException, DbException {
            PageId pid = new HeapPageId(tableId, pgNum);
            return ((HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(tupleIter == null){
                return false;
            }
            /*
            一个页面一个页面的找下去
             */
            while(curPg < numPages-1){
                if(tupleIter.hasNext())
                    return true;
                else{
                    curPg++;
                    tupleIter = getPageTuples(curPg);
                }
            }
            //curPg == numPage-1
            return tupleIter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(tupleIter == null || !tupleIter.hasNext())
                throw new NoSuchElementException("HeapFileIterator: error: next: no more elemens");
            return tupleIter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tupleIter = null;
        }
    }

}

