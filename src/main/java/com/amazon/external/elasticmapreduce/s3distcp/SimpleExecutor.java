package com.amazon.external.elasticmapreduce.s3distcp;

import java.util.concurrent.*;
import org.apache.commons.logging.*;

public class SimpleExecutor implements Executor
{
    private static final Log LOG;
    protected boolean closed;
    protected int tail;
    protected int head;
    protected Exception lastException;
    protected Runnable[] queue;
    protected Thread[] workers;
    
    public SimpleExecutor(final int queueSize, final int workerSize) {
        super();
        this.queue = new Runnable[queueSize + 1];
        this.workers = new Thread[workerSize];
        this.head = 0;
        this.tail = 0;
        this.closed = false;
        this.lastException = null;
        this.startWorkers();
    }
    
    public synchronized void registerException(final Exception e) {
        this.lastException = e;
    }
    
    public synchronized void assertNoExceptions() {
        if (this.lastException != null) {
            throw new RuntimeException("Some tasks in remote executor failed", this.lastException);
        }
    }
    
    private void startWorkers() {
        for (int i = 0; i < this.workers.length; ++i) {
            (this.workers[i] = new Thread(new Worker(this))).start();
        }
    }
    
    public void close() {
        synchronized (this) {
            this.closed = true;
            this.notifyAll();
        }
        for (int i = 0; i < this.workers.length; ++i) {
            try {
                this.workers[i].join();
            }
            catch (InterruptedException e) {
                SimpleExecutor.LOG.error((Object)"Interrupted while waiting for workers", (Throwable)e);
            }
        }
    }
    
    public synchronized boolean closed() {
        return this.closed;
    }
    
    @Override
    public synchronized void execute(final Runnable command) {
        try {
            while (this.isFull()) {
                this.wait();
            }
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.queue[this.head] = command;
        this.head = (this.head + 1) % this.queue.length;
        this.notifyAll();
    }
    
    synchronized boolean isEmpty() {
        return this.head == this.tail;
    }
    
    synchronized boolean isFull() {
        return (this.head + 1) % this.queue.length == this.tail;
    }
    
    synchronized int size() {
        final int result = this.head - this.tail;
        if (result < 0) {
            return result + this.queue.length;
        }
        return result;
    }
    
    public synchronized Runnable take() throws InterruptedException {
        while (this.isEmpty() && !this.closed) {
            this.wait(15000L);
        }
        if (!this.isEmpty()) {
            final Runnable returnItem = this.queue[this.tail];
            this.tail = (this.tail + 1) % this.queue.length;
            this.notifyAll();
            return returnItem;
        }
        return null;
    }
    
    static {
        LOG = LogFactory.getLog((Class)Worker.class);
    }
    
    static class Worker implements Runnable
    {
        private final SimpleExecutor executor;
        
        Worker(final SimpleExecutor executor) {
            super();
            this.executor = executor;
        }
        
        @Override
        public void run() {
            try {
                Runnable job;
                while ((job = this.executor.take()) != null) {
                    try {
                        job.run();
                    }
                    catch (RuntimeException e) {
                        this.executor.registerException(e);
                        SimpleExecutor.LOG.error((Object)"Worker task threw exception", (Throwable)e);
                    }
                }
            }
            catch (InterruptedException ex) {}
        }
    }
}
