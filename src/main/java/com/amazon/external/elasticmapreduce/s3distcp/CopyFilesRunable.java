package com.amazon.external.elasticmapreduce.s3distcp;

import java.security.*;
import org.apache.hadoop.fs.common.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import java.util.*;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.codec.binary.*;
import java.nio.charset.*;
import com.amazonaws.services.s3.*;
import org.apache.hadoop.fs.*;
import java.net.*;
import org.apache.commons.logging.*;

class CopyFilesRunable implements Runnable
{
    private static final Log LOG;
    private final List<FileInfo> fileInfos;
    private final CopyFilesReducer reducer;
    private final String tempPath;
    private final Path finalPath;
    
    public CopyFilesRunable(final CopyFilesReducer reducer, final List<FileInfo> fileInfos, final Path tempPath, final Path finalPath) {
        super();
        this.fileInfos = fileInfos;
        this.reducer = reducer;
        this.tempPath = tempPath.toString();
        this.finalPath = finalPath;
        CopyFilesRunable.LOG.info((Object)("Creating CopyFilesRunnable " + tempPath.toString() + ":" + finalPath.toString()));
    }
    
    private long copyStream(final InputStream inputStream, final OutputStream outputStream, final MessageDigest md) throws IOException {
        long bytesCopied = 0L;
        try {
            int len = 0;
            final byte[] buffer = new byte[this.reducer.getBufferSize()];
            while ((len = inputStream.read(buffer)) > 0) {
                md.update(buffer, 0, len);
                outputStream.write(buffer, 0, len);
                this.reducer.progress();
                bytesCopied += len;
            }
        }
        catch (Exception e) {
            throw new IOException("exception raised while copying data file", e);
        }
        return bytesCopied;
    }
    
    public ProcessedFile downloadAndMergeInputFiles() throws Exception {
        boolean finished = false;
        int numRetriesRemaining = this.reducer.getNumTransferRetries();
        byte[] digest = null;
        Path curTempPath = null;
        while (!finished && numRetriesRemaining > 0) {
            --numRetriesRemaining;
            OutputStream outputStream = null;
            curTempPath = new Path(this.tempPath + UUID.randomUUID());
            try {
                CopyFilesRunable.LOG.info((Object)("Opening temp file: " + curTempPath.toString()));
                outputStream = this.reducer.openOutputStream(curTempPath);
                final MessageDigest md = MessageDigest.getInstance("MD5");
                for (final FileInfo fileInfo : this.fileInfos) {
                    try {
                        CopyFilesRunable.LOG.info((Object)("Starting download of " + fileInfo.inputFileName + " to " + curTempPath));
                        final InputStream inputStream = this.reducer.openInputStream(new Path(fileInfo.inputFileName.toString()));
                        try {
                            final long bytesCopied = this.copyStream(inputStream, outputStream, md);
                            CopyFilesRunable.LOG.info((Object)("Copied " + bytesCopied + " bytes"));
                        }
                        finally {
                            inputStream.close();
                        }
                    }
                    catch (Exception e) {
                        if (outputStream != null && outputStream instanceof Abortable) {
                            CopyFilesRunable.LOG.warn((Object)("Output stream is abortable, aborting the output stream for " + fileInfo.inputFileName));
                            final Abortable abortable = (Abortable)outputStream;
                            abortable.abort();
                        }
                        throw e;
                    }
                    finished = true;
                    CopyFilesRunable.LOG.info((Object)("Finished downloading " + fileInfo.inputFileName));
                }
                outputStream.close();
                digest = md.digest();
                return new ProcessedFile(digest, curTempPath);
            }
            catch (Exception e2) {
                CopyFilesRunable.LOG.warn((Object)("Exception raised while copying file data to:  file=" + this.finalPath + " numRetriesRemaining=" + numRetriesRemaining), (Throwable)e2);
                try {
                    final FileSystem fs = curTempPath.getFileSystem((Configuration)this.reducer.getConf());
                    fs.delete(curTempPath, false);
                }
                catch (IOException ex) {}
                if (numRetriesRemaining <= 0) {
                    throw e2;
                }
            }
            finally {
                try {
                    outputStream.close();
                }
                catch (Exception ex2) {}
            }
        }
        return null;
    }
    
    private static File[] getTempDirs(final Configuration conf) {
        final String[] backupDirs = conf.get("fs.s3.buffer.dir").split(",");
        final List<File> tempDirs = new ArrayList<File>(backupDirs.length);
        int directoryIndex = 0;
        File result = null;
        while (directoryIndex < backupDirs.length) {
            final File dir = new File(backupDirs[directoryIndex]);
            dir.mkdirs();
            try {
                result = File.createTempFile("output-", ".tmp", dir);
                if (result != null) {
                    tempDirs.add(new File(backupDirs[directoryIndex]));
                }
                result.delete();
            }
            catch (Exception ex) {}
            ++directoryIndex;
        }
        return tempDirs.toArray(new File[0]);
    }
    
    @Override
    public void run() {
        int retriesRemaining = this.reducer.getNumTransferRetries();
        ProcessedFile processedFile = null;
        try {
            processedFile = this.downloadAndMergeInputFiles();
        }
        catch (Exception e) {
            CopyFilesRunable.LOG.warn((Object)"Error download input files. Not marking as committed", (Throwable)e);
        }
        while (retriesRemaining > 0) {
            --retriesRemaining;
            try {
                final Path curTempPath = processedFile.path;
                final FileSystem inFs = curTempPath.getFileSystem((Configuration)this.reducer.getConf());
                final FileSystem outFs = this.finalPath.getFileSystem((Configuration)this.reducer.getConf());
                if (inFs.getUri().equals(outFs.getUri())) {
                    CopyFilesRunable.LOG.info((Object)("Renaming " + curTempPath.toString() + " to " + this.finalPath.toString()));
                    inFs.mkdirs(this.finalPath.getParent());
                    inFs.rename(curTempPath, this.finalPath);
                }
                else {
                    CopyFilesRunable.LOG.info((Object)("inFs.getUri()!=outFs.getUri(): " + inFs.getUri() + "!=" + outFs.getUri()));
                    this.copyToFinalDestination(curTempPath, this.finalPath, processedFile, inFs, outFs);
                }
                for (final FileInfo fileInfo : this.fileInfos) {
                    this.reducer.markFileAsCommited(fileInfo);
                    if (this.reducer.shouldDeleteOnSuccess()) {
                        CopyFilesRunable.LOG.info((Object)("Deleting " + fileInfo.inputFileName));
                        final Path inPath = new Path(fileInfo.inputFileName.toString());
                        final FileSystem deleteFs = FileSystem.get(inPath.toUri(), (Configuration)this.reducer.getConf());
                        deleteFs.delete(inPath, false);
                    }
                }
                final Path localTempPath = new Path(this.tempPath);
                final FileSystem fs = localTempPath.getFileSystem((Configuration)this.reducer.getConf());
                fs.delete(localTempPath, true);
                return;
            }
            catch (Exception e) {
                CopyFilesRunable.LOG.warn((Object)"Error processing files. Not marking as committed", (Throwable)e);
                continue;
            }
        }
    }
    
    private void copyToFinalDestination(final Path curTempPath, final Path finalPath, final ProcessedFile processedFile, final FileSystem inFs, final FileSystem outFs) throws Exception {
        CopyFilesRunable.LOG.info((Object)("Copying " + curTempPath.toString() + " to " + finalPath.toString()));
        final byte[] digest = processedFile.checksum;
        final InputStream inStream = this.reducer.openInputStream(curTempPath);
        OutputStream outStream = null;
        if (Utils.isS3Scheme(outFs.getUri().getScheme())) {
            final FileStatus status = inFs.getFileStatus(curTempPath);
            final URI outUri = finalPath.toUri();
            final String bucket = outUri.getHost();
            final String key = outUri.getPath().substring(1);
            final ConfigurationCredentials confCreds = new ConfigurationCredentials((Configuration)this.reducer.getConf());
            final AmazonS3Client s3 = new AmazonS3Client((AWSCredentials)new BasicAWSCredentials(confCreds.getAWSAccessKeyId(), confCreds.getAWSSecretKey()));
            s3.setEndpoint(this.reducer.getConf().get("fs.s3n.endpoint", "s3.amazonaws.com"));
            final ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(status.getLen());
            if (digest != null) {
                meta.setContentMD5(new String(Base64.encodeBase64(digest), Charset.forName("UTF-8")));
            }
            if (this.reducer.shouldUseMutlipartUpload()) {
                final int chunkSize = this.reducer.getMultipartSize();
                outStream = new MultipartUploadOutputStream((AmazonS3)s3, Utils.createDefaultExecutorService(), this.reducer.getProgressable(), bucket, key, meta, chunkSize, getTempDirs((Configuration)this.reducer.getConf()));
            }
            else {
                int retries = this.reducer.getNumTransferRetries();
                while (retries > 0) {
                    try {
                        --retries;
                        CopyFilesRunable.LOG.info((Object)("S3 putObject bucket:" + bucket + " key:" + key));
                        s3.putObject(bucket, key, this.reducer.openInputStream(curTempPath), meta);
                    }
                    catch (Exception e) {
                        if (retries <= 0) {
                            throw e;
                        }
                        continue;
                    }
                    break;
                }
            }
        }
        else {
            outStream = this.reducer.openOutputStream(finalPath);
        }
        if (outStream != null) {
            final MessageDigest md = MessageDigest.getInstance("MD5");
            this.copyStream(inStream, outStream, md);
            outStream.close();
        }
        inStream.close();
    }
    
    static {
        LOG = LogFactory.getLog((Class)CopyFilesRunable.class);
    }
    
    private class ProcessedFile
    {
        public byte[] checksum;
        public Path path;
        
        public ProcessedFile(final byte[] checksum, final Path path) {
            super();
            this.checksum = checksum;
            this.path = path;
        }
    }
}
