package com.amazon.elasticmapreduce.s3distcp;

import java.io.*;

public class ByteCounterInputStream extends InputStream
{
    private final InputStream inputStream;
    private final long contentLength;
    private long bytesRead;
    private long mark;
    
    public ByteCounterInputStream(final InputStream inputStream, final long contentLength) {
        super();
        this.inputStream = inputStream;
        this.bytesRead = 0L;
        this.contentLength = contentLength;
        this.mark = 0L;
    }
    
    @Override
    public int read() throws IOException {
        final int ret = this.inputStream.read();
        if (ret >= 0) {
            ++this.bytesRead;
        }
        return ret;
    }
    
    @Override
    public int read(final byte[] b) throws IOException {
        final int ret = this.inputStream.read(b);
        if (ret >= 0) {
            this.bytesRead += ret;
        }
        return ret;
    }
    
    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        final int ret = this.inputStream.read(b, off, len);
        if (ret >= 0) {
            this.bytesRead += ret;
        }
        return ret;
    }
    
    @Override
    public long skip(final long n) throws IOException {
        final long ret = this.inputStream.skip(n);
        if (ret >= 0L) {
            this.bytesRead += ret;
        }
        return ret;
    }
    
    @Override
    public int available() throws IOException {
        return (int)(this.contentLength - this.bytesRead);
    }
    
    @Override
    public void close() throws IOException {
        this.inputStream.close();
    }
    
    @Override
    public synchronized void mark(final int readlimit) {
        this.mark = this.bytesRead;
        this.inputStream.mark(readlimit);
    }
    
    @Override
    public synchronized void reset() throws IOException {
        if (this.markSupported()) {
            this.bytesRead = this.mark;
        }
        this.inputStream.reset();
    }
    
    @Override
    public boolean markSupported() {
        return this.inputStream.markSupported();
    }
}
