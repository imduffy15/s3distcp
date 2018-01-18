package com.amazon.elasticmapreduce.s3distcp;

import org.apache.hadoop.util.*;
import java.io.*;

public class ProgressableResettableBufferedFileInputStream extends InputStream
{
    protected File file;
    protected Progressable progressable;
    private BufferedInputStream inputStream;
    private long mark;
    private long pos;
    
    public ProgressableResettableBufferedFileInputStream(final File file, final Progressable progressable) throws IOException {
        super();
        this.mark = 0L;
        this.pos = 0L;
        this.file = file;
        this.progressable = progressable;
        this.inputStream = new BufferedInputStream(new FileInputStream(file));
    }
    
    @Override
    public int available() throws IOException {
        return this.inputStream.available();
    }
    
    @Override
    public void close() throws IOException {
        this.inputStream.close();
    }
    
    @Override
    public synchronized void mark(final int readlimit) {
        if (this.progressable != null) {
            this.progressable.progress();
        }
        this.mark = this.pos;
    }
    
    @Override
    public boolean markSupported() {
        if (this.progressable != null) {
            this.progressable.progress();
        }
        return true;
    }
    
    @Override
    public int read() throws IOException {
        if (this.progressable != null) {
            this.progressable.progress();
        }
        final int read = this.inputStream.read();
        if (read != -1) {
            ++this.pos;
        }
        return read;
    }
    
    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (this.progressable != null) {
            this.progressable.progress();
        }
        final int read = this.inputStream.read(b, off, len);
        if (read != -1) {
            this.pos += read;
        }
        return read;
    }
    
    @Override
    public int read(final byte[] b) throws IOException {
        if (this.progressable != null) {
            this.progressable.progress();
        }
        final int read = this.inputStream.read(b);
        if (read != -1) {
            this.pos += read;
        }
        return read;
    }
    
    @Override
    public synchronized void reset() throws IOException {
        if (this.progressable != null) {
            this.progressable.progress();
        }
        this.inputStream.close();
        this.inputStream = new BufferedInputStream(new FileInputStream(this.file));
        this.pos = this.inputStream.skip(this.mark);
    }
    
    @Override
    public long skip(final long n) throws IOException {
        if (this.progressable != null) {
            this.progressable.progress();
        }
        final long skipped = this.inputStream.skip(n);
        this.pos += skipped;
        return skipped;
    }
}
