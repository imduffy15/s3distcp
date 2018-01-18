package com.amazon.elasticmapreduce.s3distcp;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import java.util.*;
import java.util.zip.*;
import com.google.gson.*;
import java.io.*;
import org.apache.hadoop.io.*;
import java.util.regex.*;
import org.apache.commons.logging.*;

public class FileInfoListing
{
    private static final Log LOG;
    private FileSystem fs;
    private SequenceFile.Writer writer;
    private Long fileIndex;
    private long recordIndex;
    private Long recordsInThisFile;
    private Long recordsPerFile;
    private Path tmpDir;
    private Configuration conf;
    private Path outputDir;
    private Path defaultSrcDir;
    private Pattern srcPattern;
    private Pattern groupBy;
    private OutputStream manifestStream;
    private Map<String, ManifestEntry> previousManifest;
    private final Gson gson;
    
    public FileInfoListing(final Configuration conf, final Path srcDir, final Path tmpDir, final Path outputDir, final long startingIndex, final File manifestFile, final Map<String, ManifestEntry> previousManifest) throws IOException {
        super();
        this.fileIndex = 0L;
        this.recordIndex = 0L;
        this.recordsInThisFile = 0L;
        this.conf = conf;
        this.defaultSrcDir = srcDir;
        this.tmpDir = tmpDir;
        this.outputDir = outputDir;
        this.recordsPerFile = 500000L;
        this.recordIndex = startingIndex;
        if (manifestFile != null) {
            this.manifestStream = new GZIPOutputStream(new FileOutputStream(manifestFile));
        }
        final GsonBuilder gsonBuilder = new GsonBuilder().disableHtmlEscaping();
        this.gson = gsonBuilder.create();
        this.previousManifest = previousManifest;
    }
    
    public void openNewFile() {
        try {
            if (this.writer != null) {
                this.writer.close();
            }
            ++this.fileIndex;
            this.recordsInThisFile = 0L;
            this.fs = FileSystem.get(this.tmpDir.toUri(), this.conf);
            final Path path = new Path(this.tmpDir, this.fileIndex.toString());
            FileInfoListing.LOG.info((Object)("Opening new file: " + path.toString()));
            this.writer = SequenceFile.createWriter(this.fs, this.conf, path, (Class)LongWritable.class, (Class)FileInfo.class, SequenceFile.CompressionType.NONE);
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to open new file for writing" + new Path(this.tmpDir, this.fileIndex.toString()).toString(), e);
        }
    }
    
    public void add(final Path filePath, final long fileSize) {
        this.add(filePath, this.defaultSrcDir, fileSize);
    }
    
    public void add(final Path filePath, final Path srcDir, final long fileSize) {
        final String filePathString = filePath.toString();
        if (this.srcPattern != null) {
            final Matcher matcher = this.srcPattern.matcher(filePathString);
            if (!matcher.matches()) {
                return;
            }
        }
        if (this.groupBy != null) {
            final Matcher matcher = this.groupBy.matcher(filePathString);
            if (!matcher.matches()) {
                return;
            }
            final int numGroups = matcher.groupCount();
            final StringBuilder builder = new StringBuilder();
            for (int i = 0; i < numGroups; ++i) {
                builder.append(matcher.group(i + 1));
            }
            if (builder.toString().length() == 0) {
                return;
            }
        }
        if (this.writer == null || this.recordsInThisFile > this.recordsPerFile) {
            this.openNewFile();
        }
        ++this.recordIndex;
        ++this.recordsInThisFile;
        final String outputFilePath = this.getOutputFilePath(filePath, srcDir);
        final String baseName = this.getBaseName(filePath, srcDir);
        final String manifestSrcDir = this.outputDir.toString();
        try {
            final FileInfo fileInfo = new FileInfo(this.recordIndex, filePathString, outputFilePath, fileSize);
            FileInfoListing.LOG.debug((Object)("Adding " + fileInfo));
            if (this.previousManifest == null || !this.previousManifest.containsKey(baseName) || this.previousManifest.get(baseName).size != fileSize) {
                this.writer.append((Writable)new LongWritable(this.recordIndex), (Writable)fileInfo);
            }
            if (this.manifestStream != null) {
                final ManifestEntry entry = new ManifestEntry(outputFilePath, baseName, manifestSrcDir, fileSize);
                final String outLine = this.gson.toJson((Object)entry) + "\n";
                this.manifestStream.write(outLine.getBytes("utf-8"));
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to write file copy entry " + filePathString, e);
        }
    }
    
    private String getBaseName(final Path filePath, final Path srcDir) {
        String suffix;
        final String filePathString = suffix = filePath.toString();
        final String srcDirString = srcDir.toString();
        if (filePathString.startsWith(srcDirString)) {
            suffix = filePathString.substring(srcDirString.length());
            if (suffix.startsWith("/")) {
                suffix = suffix.substring(1);
            }
        }
        return suffix;
    }
    
    private String getOutputFilePath(final Path filePath, final Path srcDir) {
        final String suffix = this.getBaseName(filePath, srcDir);
        FileInfoListing.LOG.debug((Object)("outputDir: '" + this.outputDir + "'"));
        FileInfoListing.LOG.debug((Object)("suffix: '" + suffix + "'"));
        final String outputPath = (suffix == null || suffix.isEmpty()) ? (this.outputDir.toString() + "/") : new Path(this.outputDir, suffix).toString();
        FileInfoListing.LOG.debug((Object)("Output path: '" + outputPath + "'"));
        return outputPath;
    }
    
    public void close() {
        try {
            if (this.writer != null) {
                this.writer.close();
            }
            if (this.manifestStream != null) {
                this.manifestStream.close();
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to close fileInfo writer", e);
        }
    }
    
    public Long getRecordsPerFile() {
        return this.recordsPerFile;
    }
    
    public void setRecordsPerFile(final Long recordsPerFile) {
        this.recordsPerFile = recordsPerFile;
    }
    
    public Pattern getSrcPattern() {
        return this.srcPattern;
    }
    
    public void setSrcPattern(final Pattern srcPattern) {
        this.srcPattern = srcPattern;
    }
    
    public Pattern getGroupBy() {
        return this.groupBy;
    }
    
    public void setGroupBy(final Pattern groupBy) {
        this.groupBy = groupBy;
    }
    
    public Long getFileIndex() {
        return this.fileIndex;
    }
    
    public Long getRecordIndex() {
        return this.recordIndex;
    }
    
    static {
        LOG = LogFactory.getLog((Class)FileInfoListing.class);
    }
}
