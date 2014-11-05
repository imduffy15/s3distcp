package com.amazon.external.elasticmapreduce.s3distcp;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.compress.*;
import com.hadoop.compression.lzo.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.hadoop.util.*;
import java.util.zip.*;
import org.apache.commons.logging.*;
import com.google.common.collect.*;

public class CopyFilesReducer implements Reducer<Text, FileInfo, Text, Text>
{
    private static final Log LOG;
    private static final List<String> validCodecs;
    private OutputCollector<Text, Text> collector;
    private Reporter reporter;
    private SimpleExecutor transferQueue;
    private Set<FileInfo> uncommitedFiles;
    private String tempDir;
    private long targetSize;
    private int bufferSize;
    private int numTransferRetries;
    private int multipartSize;
    private String outputCodec;
    private boolean deleteOnSuccess;
    private boolean useMultipartUpload;
    private boolean numberFiles;
    private JobConf conf;
    
    public void close() throws IOException {
        this.transferQueue.close();
        synchronized (this) {
            CopyFilesReducer.LOG.info((Object)("CopyFilesReducer uncommitted file " + this.uncommitedFiles.size()));
            for (final FileInfo fileInfo : this.uncommitedFiles) {
                CopyFilesReducer.LOG.warn((Object)("failed to upload " + fileInfo.inputFileName));
                // this.collector.collect((Object)fileInfo.outputFileName, (Object)fileInfo.inputFileName);
                this.collector.collect(fileInfo.outputFileName, fileInfo.inputFileName);
            }
            if (this.uncommitedFiles.size() > 0) {
                final String message = String.format("Reducer task failed to copy %d files: %s etc", this.uncommitedFiles.size(), this.uncommitedFiles.iterator().next().inputFileName);
                throw new RuntimeException(message);
            }
        }
    }
    
    public JobConf getConf() {
        return this.conf;
    }
    
    public boolean shouldDeleteOnSuccess() {
        return this.deleteOnSuccess;
    }
    
    public boolean shouldUseMutlipartUpload() {
        return this.useMultipartUpload;
    }
    
    public int getMultipartSize() {
        return this.multipartSize;
    }
    
    public void configure(final JobConf conf) {
        this.conf = conf;
        final int queueSize = conf.getInt("s3DistCp.copyfiles.mapper.queueSize", 10);
        final int numWorkers = conf.getInt("s3DistCp.copyfiles.mapper.numWorkers", 5);
        this.tempDir = conf.get("s3DistCp.copyfiles.reducer.tempDir", "hdfs:///tmp") + "/" + "tempspace";
        this.bufferSize = conf.getInt("s3DistCp.copyfiles.mapper.bufferSize", 1048576);
        this.targetSize = conf.getLong("s3DistCp.copyfiles.reducer.targetSize", Long.MAX_VALUE);
        this.outputCodec = conf.get("s3DistCp.copyfiles.reducer.outputCodec").toLowerCase();
        this.numberFiles = conf.getBoolean("s3DistCp.copyfiles.reducer.numberFiles", false);
        this.transferQueue = new SimpleExecutor(queueSize, numWorkers);
        this.multipartSize = conf.getInt("s3DistCp.copyFiles.multipartUploadPartSize", 16777216);
        this.uncommitedFiles = new HashSet<FileInfo>();
        this.deleteOnSuccess = conf.getBoolean("s3DistCp.copyFiles.deleteFilesOnSuccess", false);
        this.numTransferRetries = conf.getInt("s3DistCp.copyfiles.mapper.numRetries", 10);
        this.useMultipartUpload = conf.getBoolean("s3DistCp.copyFiles.useMultipartUploads", true);
    }
    
    public int getNumTransferRetries() {
        return this.numTransferRetries;
    }
    
    public int getBufferSize() {
        return this.bufferSize;
    }
    
    public boolean shouldReencodeFiles() {
        return CopyFilesReducer.validCodecs.contains(this.outputCodec);
    }
    
    private String makeFinalPath(final long fileUid, final String finalDir, String groupId, final String groupIndex) {
        final String[] groupIds = groupId.split("/");
        groupId = groupIds[groupIds.length - 1];
        if (this.numberFiles) {
            groupId = fileUid + groupId;
        }
        if (!this.outputCodec.equalsIgnoreCase("keep")) {
            String suffix;
            if (this.outputCodec.equalsIgnoreCase("gzip")) {
                suffix = groupIndex + ".gz";
            }
            else if (this.outputCodec.equalsIgnoreCase("none")) {
                suffix = groupIndex;
            }
            else {
                suffix = groupIndex + "." + this.outputCodec;
            }
            return finalDir + "/" + Utils.replaceSuffix(groupId, suffix);
        }
        String suffix = Utils.getSuffix(groupId);
        String name = groupId;
        if (groupIndex.length() > 0) {
            name = Utils.replaceSuffix(name, groupIndex);
            if (suffix.length() > 0) {
                name = name + "." + suffix;
            }
        }
        return finalDir + "/" + name;
    }
    
    public void reduce(final Text groupKey, final Iterator<FileInfo> fileInfos, final OutputCollector<Text, Text> collector, final Reporter reporter) throws IOException {
        this.collector = collector;
        this.reporter = reporter;
        long curSize = 0L;
        int groupNum = 0;
        int numFiles = 0;
        List<FileInfo> curFiles = new ArrayList<FileInfo>();
        while (fileInfos.hasNext()) {
            final FileInfo fileInfo = fileInfos.next().clone();
            ++numFiles;
            curSize += fileInfo.fileSize.get();
            curFiles.add(fileInfo);
            if (curSize >= this.targetSize) {
                final String groupId = groupKey.toString();
                final Path tempPath = new Path(this.tempDir + "/" + groupId);
                Path finalPath = new Path(fileInfo.outputFileName.toString()).getParent();
                String groupIndex = Integer.toString(groupNum);
                if (numFiles == 1 && !fileInfos.hasNext()) {
                    groupIndex = "";
                }
                finalPath = new Path(this.makeFinalPath(fileInfo.fileUID.get(), finalPath.toString(), groupId, groupIndex));
                CopyFilesReducer.LOG.info((Object)("tempPath:" + tempPath + " finalPath:" + finalPath));
                this.executeDownloads(this, curFiles, tempPath, finalPath);
                ++groupNum;
                curFiles = new ArrayList<FileInfo>();
                curSize = 0L;
            }
        }
        if (!curFiles.isEmpty()) {
            final String groupId2 = groupKey.toString();
            final Path tempPath2 = new Path(this.tempDir + "/" + UUID.randomUUID());
            final Path intermediateFinal = new Path(curFiles.get(0).outputFileName.toString()).getParent();
            CopyFilesReducer.LOG.info((Object)("tempPath:" + tempPath2 + " interPath:" + intermediateFinal));
            String groupIndex2 = Integer.toString(groupNum);
            if (numFiles == 1) {
                groupIndex2 = "";
            }
            final Path finalPath2 = new Path(this.makeFinalPath(curFiles.get(0).fileUID.get(), intermediateFinal.toString(), groupId2, groupIndex2));
            this.executeDownloads(this, curFiles, tempPath2, finalPath2);
        }
    }
    
    private void executeDownloads(final CopyFilesReducer reducer, final List<FileInfo> fileInfos, final Path tempPath, final Path finalPath) {
        synchronized (this) {
            for (final FileInfo fileInfo : fileInfos) {
                this.uncommitedFiles.add(fileInfo);
                CopyFilesReducer.LOG.info((Object)("Processing object: " + fileInfo.inputFileName.toString()));
            }
        }
        if (fileInfos.size() > 0) {
            CopyFilesReducer.LOG.info((Object)("Processing " + fileInfos.size() + " files"));
            this.transferQueue.execute(new CopyFilesRunable(reducer, fileInfos, tempPath, finalPath));
        }
        else {
            CopyFilesReducer.LOG.info((Object)"No files to process");
        }
    }
    
    public void markFileAsCommited(final FileInfo fileInfo) {
        CopyFilesReducer.LOG.info((Object)("commit " + fileInfo.inputFileName));
        synchronized (this) {
            this.uncommitedFiles.remove(fileInfo);
            this.progress();
        }
    }
    
    public InputStream openInputStream(final Path inputFilePath) throws IOException {
        final FileSystem inputFs = inputFilePath.getFileSystem((Configuration)this.conf);
        final InputStream inputStream = (InputStream)inputFs.open(inputFilePath);
        if (!this.outputCodec.equalsIgnoreCase("keep")) {
            final String suffix = Utils.getSuffix(inputFilePath.getName());
            if (suffix.equalsIgnoreCase("gz")) {
                return new GZIPInputStream(inputStream);
            }
            if (suffix.equalsIgnoreCase("snappy")) {
                final SnappyCodec codec = new SnappyCodec();
                codec.setConf((Configuration)this.getConf());
                return (InputStream)codec.createInputStream(inputStream);
            }
            if (suffix.equalsIgnoreCase("lzop") || suffix.equalsIgnoreCase("lzo")) {
                final LzopCodec codec2 = new LzopCodec();
                codec2.setConf((Configuration)this.getConf());
                return (InputStream)codec2.createInputStream(inputStream);
            }
        }
        return inputStream;
    }
    
    public OutputStream openOutputStream(final Path outputFilePath) throws IOException {
        final FileSystem outputFs = outputFilePath.getFileSystem((Configuration)this.conf);
        final OutputStream outputStream = (OutputStream)outputFs.create(outputFilePath, (Progressable)this.reporter);
        if (this.outputCodec.equalsIgnoreCase("gzip") || this.outputCodec.equalsIgnoreCase("gz")) {
            return new GZIPOutputStream(outputStream);
        }
        if (this.outputCodec.equalsIgnoreCase("lzo")) {
            final LzopCodec codec = new LzopCodec();
            codec.setConf((Configuration)this.getConf());
            return (OutputStream)codec.createOutputStream(outputStream);
        }
        if (this.outputCodec.equalsIgnoreCase("snappy")) {
            final SnappyCodec codec2 = new SnappyCodec();
            codec2.setConf((Configuration)this.getConf());
            return (OutputStream)codec2.createOutputStream(outputStream);
        }
        return outputStream;
    }
    
    public Progressable getProgressable() {
        return (Progressable)this.reporter;
    }
    
    public void progress() {
        this.reporter.progress();
    }
    
    static {
        LOG = LogFactory.getLog((Class)CopyFilesReducer.class);
        validCodecs = Lists.newArrayList("snappy", "gz", "lzo", "lzop", "gzip");
    }
}
