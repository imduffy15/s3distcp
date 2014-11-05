package com.amazon.external.elasticmapreduce.s3distcp;

import org.apache.hadoop.fs.common.*;
import com.amazonaws.services.s3.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.retry.*;
import java.util.*;
import org.apache.commons.codec.binary.*;
import java.nio.charset.*;
import java.util.concurrent.*;
import java.security.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.s3native.*;
import java.io.*;
import com.amazonaws.services.s3.model.*;

public class MultipartUploadOutputStream extends OutputStream implements Abortable
{
    public static final Log LOG;
    final AmazonS3 s3;
    final ThreadPoolExecutor threadPool;
    final Progressable progressable;
    final List<Future<PartETag>> futures;
    final File[] tempDirs;
    final String bucketName;
    final String key;
    final String uploadId;
    final long partSize;
    int partCount;
    long currentPartSize;
    File currentTemp;
    DigestOutputStream currentOutput;
    
    public MultipartUploadOutputStream(final AmazonS3 s3, final ThreadPoolExecutor threadPool, final Progressable progressable, final String bucketName, final String key, final ObjectMetadata metadata, final long partSize, final File... tempDirs) {
        super();
        this.partCount = 0;
        this.currentPartSize = 0L;
        final RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(4, 10L, TimeUnit.SECONDS);
        final Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = new HashMap<Class<? extends Exception>, RetryPolicy>();
        exceptionToPolicyMap.put(Exception.class, basePolicy);
        final RetryPolicy methodPolicy = RetryPolicies.retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL, (Map)exceptionToPolicyMap);
        final Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<String, RetryPolicy>();
        methodNameToPolicyMap.put("completeMultipartUpload", methodPolicy);
        this.s3 = (AmazonS3)RetryProxy.create((Class)AmazonS3.class, (Object)s3, (Map)methodNameToPolicyMap);
        final InitiateMultipartUploadResult result = this.s3.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key).withObjectMetadata(metadata));
        this.threadPool = threadPool;
        this.progressable = progressable;
        this.futures = new ArrayList<Future<PartETag>>();
        this.tempDirs = tempDirs;
        this.bucketName = bucketName;
        this.key = key;
        this.uploadId = result.getUploadId();
        this.partSize = partSize;
        this.setTempFileAndOutput();
    }
    
    @Override
    public void write(final byte[] b) throws IOException {
        this.write(b, 0, b.length);
    }
    
    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        long capacityLeft = this.capacityLeft();
        int offset = off;
        int length;
        int capacityLeftInt;
        for (length = len; capacityLeft < length; length -= capacityLeftInt, capacityLeft = this.capacityLeft()) {
            capacityLeftInt = (int)capacityLeft;
            this.currentOutput.write(b, offset, capacityLeftInt);
            this.kickOffUpload();
            offset += capacityLeftInt;
        }
        this.currentOutput.write(b, offset, length);
        this.currentPartSize += length;
    }
    
    @Override
    public void write(final int b) throws IOException {
        if (this.capacityLeft() < 1L) {
            this.kickOffUpload();
        }
        this.currentOutput.write(b);
        ++this.currentPartSize;
    }
    
    @Override
    public void flush() {
    }
    
    @Override
    public void close() {
        try {
            this.kickOffUpload();
            boolean anyNotDone = false;
            while (!anyNotDone) {
                anyNotDone = true;
                for (final Future<PartETag> future : this.futures) {
                    anyNotDone &= future.isDone();
                }
                if (this.progressable != null) {
                    this.progressable.progress();
                }
                Thread.sleep(1000L);
            }
            final List<PartETag> etags = new ArrayList<PartETag>();
            for (final Future<PartETag> future2 : this.futures) {
                etags.add(future2.get());
            }
            MultipartUploadOutputStream.LOG.debug((Object)("About to close multipart upload " + this.uploadId + " with bucket '" + this.bucketName + "' key '" + this.key + "' and etags '" + etags + "'"));
            this.s3.completeMultipartUpload(new CompleteMultipartUploadRequest(this.bucketName, this.key, this.uploadId, (List)etags));
        }
        catch (Exception e) {
            this.s3.abortMultipartUpload(new AbortMultipartUploadRequest(this.bucketName, this.key, this.uploadId));
            throw new RuntimeException("Error closing multipart upload", e);
        }
    }
    
    @Override
    public void abort() {
        for (final Future<PartETag> future : this.futures) {
            future.cancel(true);
        }
        this.s3.abortMultipartUpload(new AbortMultipartUploadRequest(this.bucketName, this.key, this.uploadId));
    }
    
    private void kickOffUpload() throws IOException {
        this.currentOutput.close();
        final String md5sum = new String(Base64.encodeBase64(this.currentOutput.getMessageDigest().digest()), Charset.forName("UTF-8"));
        this.futures.add(this.threadPool.submit((Callable<PartETag>)new MultipartUploadCallable(this.partCount, this.currentTemp, md5sum)));
        this.setTempFileAndOutput();
    }
    
    private long capacityLeft() {
        return this.partSize - this.currentPartSize;
    }
    
    private void setTempFileAndOutput() {
        try {
            this.currentPartSize = 0L;
            this.currentTemp = new File(this.tempDirs[this.partCount % this.tempDirs.length], "multipart-" + this.uploadId + "-" + this.partCount++);
            this.currentOutput = new DigestOutputStream(new BufferedOutputStream(new FileOutputStream(this.currentTemp)), MessageDigest.getInstance("MD5"));
        }
        catch (IOException e) {
            throw new RuntimeException("Error creating temporary output stream.", e);
        }
        catch (NoSuchAlgorithmException e2) {
            throw new RuntimeException("Error creating DigestOutputStream", e2);
        }
    }
    
    static {
        LOG = LogFactory.getLog("org.apache.hadoop.fs.s3native.MultipartUploadOutputStream");
    }
    
    private class MultipartUploadCallable implements Callable<PartETag>
    {
        private final int partNumber;
        private final File partFile;
        private final String md5sum;
        
        public MultipartUploadCallable(final int partNumber, final File partFile, final String md5sum) {
            super();
            this.partNumber = partNumber;
            this.partFile = partFile;
            this.md5sum = md5sum;
        }
        
        @Override
        public PartETag call() throws Exception {
            final InputStream is = (InputStream)new ProgressableResettableBufferedFileInputStream(this.partFile, MultipartUploadOutputStream.this.progressable);
            final UploadPartRequest request = new UploadPartRequest().withBucketName(MultipartUploadOutputStream.this.bucketName).withKey(MultipartUploadOutputStream.this.key).withUploadId(MultipartUploadOutputStream.this.uploadId).withInputStream(is).withPartNumber(this.partNumber).withPartSize(this.partFile.length()).withMD5Digest(this.md5sum);
            UploadPartResult result;
            try {
                final String message = String.format("S3 uploadPart bucket:%s key:%s part:%d size:%d", MultipartUploadOutputStream.this.bucketName, MultipartUploadOutputStream.this.key, this.partNumber, this.partFile.length());
                MultipartUploadOutputStream.LOG.info((Object)message);
                result = MultipartUploadOutputStream.this.s3.uploadPart(request);
            }
            catch (Exception e) {
                throw e;
            }
            finally {
                try {
                    if (is != null) {
                        is.close();
                    }
                }
                finally {
                    this.partFile.delete();
                }
            }
            return result.getPartETag();
        }
    }
}
