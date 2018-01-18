package com.amazon.elasticmapreduce.s3distcp;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import com.google.common.base.*;
import java.util.*;
import org.apache.hadoop.io.compress.*;
import com.hadoop.compression.lzo.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.util.zip.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import com.google.common.collect.*;

public class CopyFilesReducer extends Reducer<Text, FileInfo, Text, Text>
{
    private static final Log LOG;
    private static final List<String> validCodecs;
    private Reducer.Context context;
    private SimpleExecutor transferQueue;
    private Set<FileInfo> uncommittedFiles;
    private long targetSize;
    private int bufferSize;
    private int numTransferRetries;
    private String outputCodec;
    private String outputSuffix;
    private boolean deleteOnSuccess;
    private boolean numberFiles;
    private boolean appendToLastFile;
    private boolean group;
    private Configuration conf;
    
    protected void cleanup(final Reducer.Context context) throws IOException, InterruptedException {
        this.transferQueue.close();
        synchronized (this.uncommittedFiles) {
            if (this.uncommittedFiles.size() > 0) {
                CopyFilesReducer.LOG.warn((Object)("CopyFilesReducer uncommitted files " + this.uncommittedFiles.size()));
                for (final FileInfo fileInfo : this.uncommittedFiles) {
                    CopyFilesReducer.LOG.warn((Object)("Failed to upload " + fileInfo.inputFileName));
                    context.write((Object)fileInfo.outputFileName, (Object)fileInfo.inputFileName);
                }
                final String message = String.format("Reducer task failed to copy %d files: %s etc", this.uncommittedFiles.size(), this.uncommittedFiles.iterator().next().inputFileName);
                throw new RuntimeException(message);
            }
        }
    }
    
    public Configuration getConf() {
        return this.conf;
    }
    
    public boolean shouldDeleteOnSuccess() {
        return this.deleteOnSuccess;
    }
    
    protected void setup(final Reducer.Context context) throws IOException, InterruptedException {
        this.conf = context.getConfiguration();
        final int queueSize = this.conf.getInt("s3DistCp.copyfiles.mapper.queueSize", 10);
        final int numWorkers = this.conf.getInt("s3DistCp.copyfiles.mapper.numWorkers", 5);
        this.bufferSize = this.conf.getInt("s3DistCp.copyfiles.mapper.bufferSize", 1048576);
        this.targetSize = this.conf.getLong("s3DistCp.copyfiles.reducer.targetSize", Long.MAX_VALUE);
        this.outputCodec = this.conf.get("s3DistCp.copyfiles.reducer.outputCodec").toLowerCase();
        this.numberFiles = this.conf.getBoolean("s3DistCp.copyfiles.reducer.numberFiles", false);
        this.transferQueue = new SimpleExecutor(queueSize, numWorkers);
        this.uncommittedFiles = Collections.synchronizedSet(new HashSet<FileInfo>());
        this.deleteOnSuccess = this.conf.getBoolean("s3DistCp.copyFiles.deleteFilesOnSuccess", false);
        this.numTransferRetries = this.conf.getInt("s3DistCp.copyfiles.mapper.numRetries", 10);
        this.appendToLastFile = this.conf.getBoolean("s3distcp.copyFiles.appendToLastFile", false);
        this.group = (this.conf.get("s3DistCp.listfiles.groupByPattern", (String)null) != null);
    }
    
    public int getNumTransferRetries() {
        return this.numTransferRetries;
    }
    
    public int getBufferSize() {
        return this.bufferSize;
    }
    
    private String makeFinalPath(final long fileUid, final String finalDir, String groupId, final String groupIndex) {
        final String[] groupIds = groupId.split("/");
        groupId = groupIds[groupIds.length - 1];
        if (this.numberFiles) {
            groupId = fileUid + groupId;
        }
        String suffix;
        if (!Strings.isNullOrEmpty(this.outputSuffix)) {
            suffix = groupIndex + "." + this.outputSuffix;
        }
        else {
            suffix = groupIndex;
        }
        final String groupIdSuffix = Utils.getSuffix(groupId);
        if (!Strings.isNullOrEmpty(groupIdSuffix) && CopyFilesReducer.validCodecs.contains(groupIdSuffix)) {
            return finalDir + "/" + Utils.replaceSuffix(groupId, suffix);
        }
        return finalDir + "/" + Utils.appendSuffix(groupId, suffix);
    }
    
    private String determineOutputSuffix(final List<FileInfo> fileInfos) {
        if (this.outputCodec.equalsIgnoreCase("keep")) {
            String codec = null;
            for (final FileInfo fileInfo : fileInfos) {
                String currentSuffix = Utils.getSuffix(fileInfo.inputFileName.toString()).toLowerCase();
                if (!CopyFilesReducer.validCodecs.contains(currentSuffix)) {
                    currentSuffix = "";
                }
                if (codec == null) {
                    codec = currentSuffix;
                }
                else {
                    if (!codec.equals(currentSuffix)) {
                        throw new RuntimeException("Cannot keep compression scheme for input files with different compression schemes.");
                    }
                    continue;
                }
            }
            if (codec == null) {
                return null;
            }
            final String s = codec;
            switch (s) {
                case "gz":
                case "gzip": {
                    return "gz";
                }
                case "lzo":
                case "lzop": {
                    return "lzo";
                }
                case "snappy": {
                    return "snappy";
                }
                case "": {
                    return "";
                }
                default: {
                    throw new RuntimeException("Unsupported output codec: " + codec);
                }
            }
        }
        else {
            if (this.outputCodec.equalsIgnoreCase("none")) {
                String suffix = null;
                for (final FileInfo fileInfo : fileInfos) {
                    final String currentSuffix = Utils.getSuffix(fileInfo.inputFileName.toString()).toLowerCase();
                    if (suffix == null) {
                        suffix = currentSuffix;
                    }
                    else {
                        if (!suffix.equals(currentSuffix)) {
                            return "";
                        }
                        continue;
                    }
                }
                if (CopyFilesReducer.validCodecs.contains(suffix)) {
                    suffix = "";
                }
                return suffix;
            }
            return this.outputCodec.toLowerCase();
        }
    }
    
    protected void reduce(final Text groupKey, final Iterable<FileInfo> fileInfos, final Reducer.Context context) throws IOException, InterruptedException {
        this.context = context;
        long curSize = 0L;
        int groupNum = -1;
        final List<FileInfo> allFiles = new ArrayList<FileInfo>();
        List<FileInfo> curFiles = new ArrayList<FileInfo>();
        final String groupId = Utils.cleanupColonsAndSlashes(groupKey.toString());
        Path finalPath = null;
        while (fileInfos.iterator().hasNext()) {
            allFiles.add(fileInfos.iterator().next().clone());
        }
        this.outputSuffix = this.determineOutputSuffix(allFiles);
        CopyFilesReducer.LOG.info((Object)("Output suffix: '" + this.outputSuffix + "'"));
        for (int i = 0; i < allFiles.size(); ++i) {
            final FileInfo fileInfo = allFiles.get(i).clone();
            curSize += fileInfo.fileSize.get();
            curFiles.add(fileInfo);
            final Path parentDir = new Path(fileInfo.outputFileName.toString()).getParent();
            if (finalPath == null) {
                do {
                    ++groupNum;
                    finalPath = new Path(this.makeFinalPath(fileInfo.fileUID.get(), parentDir.toString(), groupId, this.getGroupIndex(groupNum)));
                } while (finalPath.getFileSystem(this.getConf()).exists(finalPath) && this.group);
                if (this.appendToLastFile && this.group) {
                    final Path tempPath = new Path(this.makeFinalPath(fileInfo.fileUID.get(), parentDir.toString(), groupId, this.getGroupIndex(groupNum - 1)));
                    if (tempPath.getFileSystem(this.getConf()).exists(tempPath)) {
                        final long tempFileSize = tempPath.getFileSystem(this.getConf()).getFileStatus(tempPath).getLen();
                        if (tempFileSize + curSize < this.targetSize) {
                            curSize += tempFileSize;
                            finalPath = tempPath;
                            --groupNum;
                        }
                    }
                }
            }
            if (curSize >= this.targetSize || i == allFiles.size() - 1) {
                CopyFilesReducer.LOG.info((Object)("finalPath:" + finalPath));
                this.executeDownloads(this, curFiles, finalPath);
                curFiles = new ArrayList<FileInfo>();
                curSize = 0L;
                ++groupNum;
                finalPath = new Path(this.makeFinalPath(fileInfo.fileUID.get(), parentDir.toString(), groupId, this.getGroupIndex(groupNum)));
            }
        }
    }
    
    private String getGroupIndex(final int groupNum) {
        if (groupNum == 0) {
            return "";
        }
        return Integer.toString(groupNum);
    }
    
    private void executeDownloads(final CopyFilesReducer reducer, final List<FileInfo> fileInfos, final Path finalPath) {
        this.uncommittedFiles.addAll(fileInfos);
        for (final FileInfo fileInfo : fileInfos) {
            CopyFilesReducer.LOG.info((Object)("Processing object: " + fileInfo.inputFileName.toString()));
        }
        if (fileInfos.size() > 0) {
            CopyFilesReducer.LOG.info((Object)("Processing " + fileInfos.size() + " files"));
            this.transferQueue.execute(new CopyFilesRunnable(reducer, fileInfos, finalPath));
        }
        else {
            CopyFilesReducer.LOG.info((Object)"No files to process");
        }
    }
    
    public void markFilesAsCommitted(final List<FileInfo> fileInfos) {
        synchronized (this.uncommittedFiles) {
            CopyFilesReducer.LOG.info((Object)("Marking " + fileInfos.size() + " files as committed"));
            for (final FileInfo fileInfo : fileInfos) {
                CopyFilesReducer.LOG.info((Object)("Committing file: " + fileInfo.inputFileName));
            }
            this.uncommittedFiles.removeAll(fileInfos);
        }
        this.progress();
    }
    
    public InputStream decorateInputStream(final InputStream inputStream, final Path inputFilePath) throws IOException {
        final String suffix = Utils.getSuffix(inputFilePath.getName()).toLowerCase();
        if (suffix.equals("gz") || suffix.equals("gzip")) {
            final FileSystem inputFs = inputFilePath.getFileSystem(this.conf);
            return new GZIPInputStream(new ByteCounterInputStream(inputStream, inputFs.getFileStatus(inputFilePath).getLen()));
        }
        if (suffix.equals("snappy")) {
            final SnappyCodec codec = new SnappyCodec();
            codec.setConf(this.getConf());
            return (InputStream)codec.createInputStream(inputStream);
        }
        if (suffix.equals("lzop") || suffix.equals("lzo")) {
            final LzopCodec codec2 = new LzopCodec();
            codec2.setConf(this.getConf());
            return (InputStream)codec2.createInputStream(inputStream);
        }
        return inputStream;
    }
    
    public InputStream openInputStream(final Path inputFilePath) throws IOException {
        final FileSystem inputFs = inputFilePath.getFileSystem(this.conf);
        return (InputStream)inputFs.open(inputFilePath);
    }
    
    public OutputStream decorateOutputStream(final OutputStream outputStream, final Path outputFilePath) throws IOException {
        final String suffix = Utils.getSuffix(outputFilePath.getName()).toLowerCase();
        if ("gz".equals(suffix) || "gzip".equals(suffix)) {
            return new GZIPOutputStream(outputStream);
        }
        if ("lzo".equals(suffix) || "lzop".equals(suffix)) {
            final LzopCodec codec = new LzopCodec();
            codec.setConf(this.getConf());
            return (OutputStream)codec.createOutputStream(outputStream);
        }
        if ("snappy".equals(suffix)) {
            final SnappyCodec codec2 = new SnappyCodec();
            codec2.setConf(this.getConf());
            return (OutputStream)codec2.createOutputStream(outputStream);
        }
        return outputStream;
    }
    
    public OutputStream openOutputStream(final Path outputFilePath) throws IOException {
        final FileSystem outputFs = outputFilePath.getFileSystem(this.conf);
        OutputStream outputStream;
        if (!Utils.isS3Scheme(outputFilePath.getFileSystem(this.conf).getUri().getScheme()) && outputFs.exists(outputFilePath) && this.appendToLastFile) {
            outputStream = (OutputStream)outputFs.append(outputFilePath);
        }
        else {
            outputStream = (OutputStream)outputFs.create(outputFilePath);
        }
        return outputStream;
    }
    
    public Progressable getProgressable() {
        return (Progressable)this.context;
    }
    
    public void progress() {
        this.context.progress();
    }
    
    static {
        LOG = LogFactory.getLog((Class)CopyFilesReducer.class);
        validCodecs = Lists.newArrayList(new String[] { "snappy", "gz", "lzo", "lzop", "gzip" });
    }
}
