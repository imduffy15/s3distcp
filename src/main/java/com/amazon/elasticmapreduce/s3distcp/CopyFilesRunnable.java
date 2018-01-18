package com.amazon.elasticmapreduce.s3distcp;

import java.io.*;
import org.apache.hadoop.fs.*;
import java.util.*;
import org.apache.hadoop.fs.common.*;
import org.apache.commons.logging.*;

class CopyFilesRunnable implements Runnable
{
    private static final Log LOG;
    private final List<FileInfo> inputFiles;
    private final Path outputFile;
    private final CopyFilesReducer reducer;
    
    public CopyFilesRunnable(final CopyFilesReducer reducer, final List<FileInfo> inputFiles, final Path outputFile) {
        super();
        this.inputFiles = inputFiles;
        this.outputFile = outputFile;
        this.reducer = reducer;
        CopyFilesRunnable.LOG.info((Object)("Creating CopyFilesRunnable: " + outputFile));
    }
    
    @Override
    public void run() {
        try {
            this.mergeAndCopyFiles();
        }
        catch (Exception e) {
            CopyFilesRunnable.LOG.error((Object)"Error downloading input files. Not marking as committed", (Throwable)e);
            return;
        }
        this.reducer.markFilesAsCommitted(this.inputFiles);
        if (this.reducer.shouldDeleteOnSuccess()) {
            this.deleteOnSuccess();
        }
    }
    
    private long copyStream(final InputStream inputStream, final OutputStream outputStream) throws IOException {
        long bytesCopied = 0L;
        try {
            final byte[] buffer = new byte[this.reducer.getBufferSize()];
            int len;
            while ((len = inputStream.read(buffer)) > 0) {
                outputStream.write(buffer, 0, len);
                this.reducer.progress();
                bytesCopied += len;
            }
        }
        catch (Exception e) {
            throw new IOException("Exception raised while copying data file", e);
        }
        return bytesCopied;
    }
    
    private boolean deleteWithRetries(final Path file, final boolean recursive) throws IOException {
        int retriesRemaining = this.reducer.getNumTransferRetries();
        while (true) {
            try {
                final FileSystem outFs = file.getFileSystem(this.reducer.getConf());
                return outFs.delete(file, recursive);
            }
            catch (IOException e) {
                --retriesRemaining;
                CopyFilesRunnable.LOG.warn((Object)("Exception raised while attempting to delete=" + file + " numRetriesRemaining=" + retriesRemaining), (Throwable)e);
                if (retriesRemaining <= 0) {
                    throw e;
                }
            }
        }
    }
    
    private void deleteOnSuccess() {
        for (final FileInfo inputFile : this.inputFiles) {
            CopyFilesRunnable.LOG.info((Object)("Deleting " + inputFile.inputFileName));
            final Path inPath = new Path(inputFile.inputFileName.toString());
            try {
                this.deleteWithRetries(inPath, false);
            }
            catch (IOException e) {
                CopyFilesRunnable.LOG.error((Object)("Failed to delete file " + inputFile.inputFileName + ". Skipping."), (Throwable)e);
            }
        }
    }
    
    private void mergeAndCopyFiles() throws IOException {
        int retriesRemaining = this.reducer.getNumTransferRetries();
        while (true) {
            try (final OutputStream outputStream = this.reducer.decorateOutputStream(this.reducer.openOutputStream(this.outputFile), this.outputFile)) {
                CopyFilesRunnable.LOG.info((Object)("Opening output file: " + this.outputFile));
                for (final FileInfo inputFile : this.inputFiles) {
                    final Path inputFilePath = new Path(inputFile.inputFileName.toString());
                    try (final InputStream inputStream = this.reducer.decorateInputStream(this.reducer.openInputStream(inputFilePath), inputFilePath)) {
                        CopyFilesRunnable.LOG.info((Object)("Starting download of " + inputFile.inputFileName + " to " + this.outputFile));
                        final long bytesCopied = this.copyStream(inputStream, outputStream);
                        CopyFilesRunnable.LOG.info((Object)("Copied " + bytesCopied + " bytes"));
                    }
                    catch (Exception e) {
                        this.handlePartialOutput(this.outputFile, outputStream);
                        throw e;
                    }
                    CopyFilesRunnable.LOG.info((Object)("Finished downloading " + inputFile.inputFileName));
                }
                CopyFilesRunnable.LOG.info((Object)("Finished downloading " + this.inputFiles.size() + " input file(s) to " + this.outputFile));
            }
            catch (IOException e2) {
                --retriesRemaining;
                CopyFilesRunnable.LOG.warn((Object)("Exception raised while copying file data to file=" + this.outputFile + " numRetriesRemaining=" + retriesRemaining), (Throwable)e2);
                if (retriesRemaining <= 0) {
                    throw e2;
                }
                continue;
            }
            break;
        }
    }
    
    private void handlePartialOutput(final Path outputFile, final OutputStream outputStream) {
        if (outputStream != null) {
            if (outputStream instanceof Abortable) {
                CopyFilesRunnable.LOG.warn((Object)("Output stream is abortable, aborting the output stream for " + outputFile));
                final Abortable abortable = (Abortable)outputStream;
                try {
                    abortable.abort();
                    outputStream.close();
                }
                catch (IOException e) {
                    CopyFilesRunnable.LOG.warn((Object)("Failed to abort the output stream for " + outputFile));
                }
            }
            else {
                try {
                    outputStream.close();
                }
                catch (IOException e2) {
                    CopyFilesRunnable.LOG.warn((Object)("Failed to close the output stream for " + outputFile));
                }
                CopyFilesRunnable.LOG.warn((Object)("Output stream is not abortable, attempting to delete partial output written to " + outputFile));
                try {
                    this.deleteWithRetries(outputFile, false);
                }
                catch (Exception e3) {
                    CopyFilesRunnable.LOG.warn((Object)("Failed to delete partial output stored in " + outputFile));
                }
            }
        }
    }
    
    static {
        LOG = LogFactory.getLog((Class)CopyFilesRunnable.class);
    }
}
