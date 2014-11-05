package com.amazon.external.elasticmapreduce.s3distcp;

import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import java.net.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.auth.*;
import com.amazonaws.*;
import com.amazonaws.services.s3.model.*;
import emr.hbase.options.*;
import com.google.gson.*;
import java.util.zip.*;
import org.apache.hadoop.fs.*;
import java.util.*;
import java.io.*;
import java.util.regex.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.commons.logging.*;

public class S3DistCp implements Tool
{
    private static final Log LOG;
    private static final int MAX_LIST_RETRIES = 10;
    private Configuration conf;
    
    public void createInputFileList(final Configuration conf, final Path srcPath, final FileInfoListing fileInfoListing) {
        final URI srcUri = srcPath.toUri();
        if (srcUri.getScheme().equals("s3") || srcUri.getScheme().equals("s3n")) {
            this.createInputFileListS3(conf, srcUri, fileInfoListing);
        }
        else {
            try {
                final FileSystem fs = srcPath.getFileSystem(conf);
                final Queue<Path> pathsToVisit = new ArrayDeque<Path>();
                pathsToVisit.add(srcPath);
                while (pathsToVisit.size() > 0) {
                    final Path curPath = pathsToVisit.remove();
                    final FileStatus[] arr$;
                    final FileStatus[] statuses = arr$ = fs.listStatus(curPath);
                    for (final FileStatus status : arr$) {
                        if (status.isDir()) {
                            pathsToVisit.add(status.getPath());
                        }
                        else {
                            fileInfoListing.add(status.getPath(), status.getLen());
                        }
                    }
                }
            }
            catch (IOException e) {
                S3DistCp.LOG.fatal((Object)"Failed to list input files", (Throwable)e);
                System.exit(-4);
            }
        }
    }
    
    public void createInputFileListS3(final Configuration conf, final URI srcUri, final FileInfoListing fileInfoListing) {
        final AmazonS3Client s3Client = new AmazonS3Client((AWSCredentials)new ConfigurationCredentials(conf));
        ObjectListing objects = null;
        boolean finished = false;
        int retryCount = 0;
        final String scheme = srcUri.getScheme() + "://";
        while (!finished) {
            final ListObjectsRequest listObjectRequest = new ListObjectsRequest().withBucketName(srcUri.getHost());
            if (srcUri.getPath().length() > 1) {
                listObjectRequest.setPrefix(srcUri.getPath().substring(1));
            }
            if (objects != null) {
                listObjectRequest.withMaxKeys(1000).withMarker(objects.getNextMarker());
            }
            try {
                objects = s3Client.listObjects(listObjectRequest);
                retryCount = 0;
            }
            catch (AmazonClientException e) {
                if (++retryCount > 10) {
                    S3DistCp.LOG.fatal((Object)"Failed to list objects", (Throwable)e);
                    throw e;
                }
                S3DistCp.LOG.warn((Object)("Error listing objects: " + e.getMessage()), (Throwable)e);
                continue;
            }
            for (final S3ObjectSummary object : objects.getObjectSummaries()) {
                if (object.getKey().endsWith("/")) {
                    S3DistCp.LOG.info((Object)("Skipping key '" + object.getKey() + "' because it ends with '/'"));
                }
                else {
                    final String s3FilePath = scheme + object.getBucketName() + "/" + object.getKey();
                    S3DistCp.LOG.debug((Object)("About to add " + s3FilePath));
                    fileInfoListing.add(new Path(s3FilePath), object.getSize());
                }
            }
            if (!objects.isTruncated()) {
                finished = true;
            }
        }
    }
    
    public int run(final String[] args) {
        final emr.hbase.options.Options options = new emr.hbase.options.Options();
        final SimpleOption helpOption = options.noArg("--help", "Print help text");
        final OptionWithArg srcOption = options.withArg("--src", "Directory to copy files from");
        final OptionWithArg destOption = options.withArg("--dest", "Directory to copy files to");
        final OptionWithArg tmpDirOption = options.withArg("--tmpDir", "Temporary directory location");
        final OptionWithArg srcPatternOption = options.withArg("--srcPattern", "Include only source files matching this pattern");
        final OptionWithArg filePerMapperOption = options.withArg("--filesPerMapper", "Place up to this number of files in each map task");
        final OptionWithArg groupByPatternOption = options.withArg("--groupBy", "Pattern to group input files by");
        final OptionWithArg targetSizeOption = options.withArg("--targetSize", "Target size for output files");
        final OptionWithArg outputCodecOption = options.withArg("--outputCodec", "Compression codec for output files");
        final OptionWithArg s3EndpointOption = options.withArg("--s3Endpoint", "S3 endpoint to use for uploading files");
        final SimpleOption deleteOnSuccessOption = options.noArg("--deleteOnSuccess", "Delete input files after a successful copy");
        final SimpleOption disableMultipartUploadOption = options.noArg("--disableMultipartUpload", "Disable the use of multipart upload");
        final OptionWithArg multipartUploadPartSizeOption = options.withArg("--multipartUploadChunkSize", "The size in MiB of the multipart upload part size");
        final OptionWithArg startingIndexOption = options.withArg("--startingIndex", "The index to start with for file numbering");
        final SimpleOption numberFilesOption = options.noArg("--numberFiles", "Prepend sequential numbers the file names");
        final OptionWithArg outputManifest = options.withArg("--outputManifest", "The name of the manifest file");
        final OptionWithArg previousManifest = options.withArg("--previousManifest", "The path to an existing manifest file");
        final SimpleOption copyFromManifest = options.noArg("--copyFromManifest", "Copy from a manifest instead of listing a directory");
        options.parseArguments(args);
        if (helpOption.defined()) {
            S3DistCp.LOG.info((Object)options.helpText());
            return 0;
        }
        srcOption.require();
        destOption.require();
        final S3DistCpOptions s = new S3DistCpOptions();
        if (srcOption.defined()) {
            s.setSrcPath(srcOption.value);
        }
        if (tmpDirOption.defined()) {
            s.setTmpDir(tmpDirOption.value);
        }
        if (destOption.defined()) {
            s.setDest(destOption.value);
        }
        if (numberFilesOption.defined()) {
            s.setNumberFiles(numberFilesOption.value);
        }
        if (srcPatternOption.defined()) {
            s.setSrcPattern(srcPatternOption.value);
        }
        if (filePerMapperOption.defined()) {
            s.setFilePerMapper(filePerMapperOption.value);
        }
        if (groupByPatternOption.defined()) {
            s.setGroupByPattern(groupByPatternOption.value);
        }
        if (targetSizeOption.defined()) {
            s.setTargetSize(targetSizeOption.value);
        }
        if (outputCodecOption.defined()) {
            s.setOutputCodec(outputCodecOption.value);
        }
        if (s3EndpointOption.defined()) {
            s.setS3Endpoint(s3EndpointOption.value);
        }
        if (deleteOnSuccessOption.defined()) {
            s.setDeleteOnSuccess(deleteOnSuccessOption.value);
        }
        if (disableMultipartUploadOption.defined()) {
            s.setDisableMultipartUpload(disableMultipartUploadOption.value);
        }
        if (multipartUploadPartSizeOption.defined()) {
            s.setMultipartUploadPartSize(multipartUploadPartSizeOption.value);
        }
        if (startingIndexOption.defined()) {
            s.setStartingIndex(startingIndexOption.value);
        }
        if (numberFilesOption.defined()) {
            s.setNumberFiles(numberFilesOption.value);
        }
        if (outputManifest.defined()) {
            s.setManifestPath(outputManifest.value);
        }
        if (previousManifest.defined()) {
            s.setPreviousManifest(loadManifest(new Path(previousManifest.value), this.getConf()));
        }
        if (copyFromManifest.defined()) {
            s.setCopyFromManifest(true);
        }
        return this.run(s);
    }
    
    public static Map<String, ManifestEntry> loadManifest(final Path manifestPath, final Configuration config) {
        final Gson gson = new Gson();
        Map<String, ManifestEntry> manifest = null;
        try {
            manifest = new TreeMap<String, ManifestEntry>();
            final FileSystem fs = FileSystem.get(manifestPath.toUri(), config);
            final FSDataInputStream inStream = fs.open(manifestPath);
            final GZIPInputStream gzipStream = new GZIPInputStream((InputStream)inStream);
            final Scanner scanner = new Scanner(gzipStream);
            manifest = new TreeMap<String, ManifestEntry>();
            while (scanner.hasNextLine()) {
                final String line = scanner.nextLine();
                final ManifestEntry entry = gson.fromJson(line, ManifestEntry.class);
                manifest.put(entry.baseName, entry);
            }
        }
        catch (Exception e) {
            S3DistCp.LOG.error((Object)("Failed to load manifest '" + manifestPath + "'"));
        }
        return manifest;
    }
    
    public int run(final S3DistCpOptions options) {
        final JobConf job = new JobConf(this.getConf(), (Class)S3DistCp.class);
        Path srcPath = new Path(options.getSrcPath());
        if (!srcPath.isAbsolute()) {
            S3DistCp.LOG.fatal((Object)"Source path must be absolute");
            System.exit(5);
        }
        try {
            final FileSystem fs = FileSystem.get(srcPath.toUri(), (Configuration)job);
            srcPath = fs.getFileStatus(srcPath).getPath();
        }
        catch (Exception e) {
            S3DistCp.LOG.fatal((Object)"Failed to get source file system", (Throwable)e);
            throw new RuntimeException("Failed to get source file system", e);
        }
        job.set("s3DistCp.copyfiles.srcDir", srcPath.toString());
        String tempDirRoot = job.get("s3DistCp.copyfiles.reducer.tempDir", options.getTmpDir());
        if (tempDirRoot == null) {
            tempDirRoot = "hdfs:///tmp";
        }
        tempDirRoot = tempDirRoot + "/" + UUID.randomUUID();
        final Path outputPath = new Path(tempDirRoot, "output");
        final Path inputPath = new Path(tempDirRoot, "files");
        final Path tempPath = new Path(tempDirRoot, "tempspace");
        final Path destPath = new Path(options.getDest());
        if (!destPath.isAbsolute()) {
            S3DistCp.LOG.fatal((Object)"Destination path must be absolute");
            System.exit(4);
        }
        job.set("s3DistCp.copyfiles.reducer.tempDir", tempDirRoot);
        S3DistCp.LOG.info((Object)("Using output path '" + outputPath.toString() + "'"));
        job.set("s3DistCp.copyfiles.destDir", destPath.toString());
        job.setBoolean("s3DistCp.copyfiles.reducer.numberFiles", (boolean)options.getNumberFiles());
        this.deleteRecursive((Configuration)job, inputPath);
        this.deleteRecursive((Configuration)job, outputPath);
        FileInfoListing fileInfoListing = null;
        File manifestFile = null;
        if (options.getManifestPath() != null) {
            manifestFile = new File(options.getManifestPath());
        }
        try {
            Map<String, ManifestEntry> previousManifest = null;
            if (!options.copyFromManifest) {
                previousManifest = options.getPreviousManifest();
            }
            fileInfoListing = new FileInfoListing((Configuration)job, srcPath, inputPath, destPath, options.getStartingIndex(), manifestFile, previousManifest);
        }
        catch (IOException e2) {
            S3DistCp.LOG.fatal((Object)"Error initializing manifest file", (Throwable)e2);
            System.exit(5);
        }
        if (options.getSrcPattern() != null) {
            fileInfoListing.setSrcPattern(Pattern.compile(options.getSrcPattern()));
        }
        if (options.getGroupByPattern() != null) {
            final String groupByPattern = options.getGroupByPattern();
            if (!groupByPattern.contains("(") || !groupByPattern.contains(")")) {
                S3DistCp.LOG.fatal((Object)"Group by pattern must contain at least one group.  Use () to enclose a group");
                System.exit(1);
            }
            try {
                fileInfoListing.setGroupBy(Pattern.compile(groupByPattern));
                job.set("s3DistCp.listfiles.gropubypattern", groupByPattern);
            }
            catch (Exception e4) {
                System.err.println("Invalid group by pattern");
                System.exit(1);
            }
        }
        if (options.getFilePerMapper() != null) {
            fileInfoListing.setRecordsPerFile(options.getFilePerMapper());
        }
        if (options.getS3Endpoint() != null) {
            job.set("fs.s3n.endpoint", options.getS3Endpoint());
        }
        job.setBoolean("s3DistCp.copyFiles.useMultipartUploads", !options.getDisableMultipartUpload());
        if (options.getMultipartUploadPartSize() != null) {
            final Integer partSize = options.getMultipartUploadPartSize();
            job.setInt("s3DistCp.copyFiles.multipartUploadPartSize", partSize * 1024 * 1024);
        }
        try {
            if (options.getCopyFromManifest() && options.getPreviousManifest() != null) {
                for (final ManifestEntry entry : options.getPreviousManifest().values()) {
                    fileInfoListing.add(new Path(entry.path), new Path(entry.srcDir), entry.size);
                }
            }
            else {
                this.createInputFileList((Configuration)job, srcPath, fileInfoListing);
            }
            S3DistCp.LOG.info((Object)("Created " + fileInfoListing.getFileIndex() + " files to copy " + fileInfoListing.getRecordIndex() + " files "));
        }
        finally {
            fileInfoListing.close();
        }
        job.setJobName("S3DistCp: " + srcPath.toString() + " -> " + destPath.toString());
        job.setReduceSpeculativeExecution(false);
        if (options.getTargetSize() != null) {
            try {
                final long targetSize = options.getTargetSize();
                job.setLong("s3DistCp.copyfiles.reducer.targetSize", targetSize * 1024L * 1024L);
            }
            catch (Exception e5) {
                System.err.println("Error parsing target file size");
                System.exit(2);
            }
        }
        final String outputCodec = options.getOutputCodec();
        job.set("s3DistCp.copyfiles.reducer.outputCodec", outputCodec);
        job.setBoolean("s3DistCp.copyFiles.deleteFilesOnSuccess", (boolean)options.getDeleteOnSuccess());
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormat((Class)SequenceFileInputFormat.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)FileInfo.class);
        job.setMapperClass((Class)GroupFilesMapper.class);
        job.setReducerClass((Class)CopyFilesReducer.class);
        job.setOutputFormat((Class)TextOutputFormat.class);
        try {
            final RunningJob runningJob = JobClient.runJob(job);
            this.deleteRecursiveNoThrow((Configuration)job, tempPath);
            final Counters counters = runningJob.getCounters();
            final Counters.Group group = counters.getGroup("org.apache.hadoop.mapred.Task$Counter");
            final long reduceOutputRecords = group.getCounterForName("REDUCE_OUTPUT_RECORDS").getValue();
            if (reduceOutputRecords > 0L) {
                S3DistCp.LOG.error((Object)(reduceOutputRecords + " files failed to copy"));
                throw new RuntimeException(reduceOutputRecords + " files failed to copy");
            }
            final FileSystem tempFs = FileSystem.get(tempPath.toUri(), (Configuration)job);
            tempFs.delete(tempPath, true);
            if (manifestFile != null) {
                final FileSystem destFs = FileSystem.get(destPath.toUri(), (Configuration)job);
                destFs.copyFromLocalFile(new Path(manifestFile.getAbsolutePath()), destPath);
                manifestFile.delete();
            }
        }
        catch (IOException e3) {
            this.deleteRecursiveNoThrow((Configuration)job, tempPath);
            throw new RuntimeException("Error running job", e3);
        }
        return 0;
    }
    
    private void deleteRecursiveNoThrow(final Configuration conf, final Path path) {
        S3DistCp.LOG.info((Object)("Try to recursively delete " + path.toString()));
        try {
            FileSystem.get(path.toUri(), conf).delete(path, true);
        }
        catch (IOException e) {
            S3DistCp.LOG.info((Object)("Failed to recursively delete " + path.toString()));
        }
    }
    
    private void deleteRecursive(final Configuration conf, final Path outputPath) {
        try {
            FileSystem.get(outputPath.toUri(), conf).delete(outputPath, true);
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to delete directory " + outputPath.toString(), e);
        }
    }
    
    public Configuration getConf() {
        return this.conf;
    }
    
    public void setConf(final Configuration conf) {
        this.conf = conf;
    }
    
    static {
        LOG = LogFactory.getLog((Class)S3DistCp.class);
    }
    
    public static class S3DistCpOptions
    {
        String srcPath;
        String tmpDir;
        String dest;
        boolean numberFiles;
        String srcPattern;
        Long filePerMapper;
        String groupByPattern;
        Integer targetSize;
        String outputCodec;
        String s3Endpoint;
        boolean deleteOnSuccess;
        boolean disableMultipartUpload;
        String manifestPath;
        Integer multipartUploadPartSize;
        Long startingIndex;
        Map<String, ManifestEntry> previousManifest;
        Boolean copyFromManifest;
        
        public S3DistCpOptions() {
            super();
            this.numberFiles = false;
            this.outputCodec = "keep";
            this.deleteOnSuccess = false;
            this.disableMultipartUpload = false;
            this.startingIndex = 0L;
            this.copyFromManifest = false;
        }
        
        public String getSrcPath() {
            return this.srcPath;
        }
        
        public void setSrcPath(final String srcPath) {
            this.srcPath = srcPath;
        }
        
        public String getTmpDir() {
            return this.tmpDir;
        }
        
        public void setTmpDir(final String tmpDir) {
            this.tmpDir = tmpDir;
        }
        
        public String getDest() {
            return this.dest;
        }
        
        public void setDest(final String dest) {
            this.dest = dest;
        }
        
        public Boolean getNumberFiles() {
            return this.numberFiles;
        }
        
        public void setNumberFiles(final Boolean numberFiles) {
            this.numberFiles = numberFiles;
        }
        
        public String getSrcPattern() {
            return this.srcPattern;
        }
        
        public void setSrcPattern(final String srcPattern) {
            this.srcPattern = srcPattern;
        }
        
        public Long getFilePerMapper() {
            return this.filePerMapper;
        }
        
        public void setFilePerMapper(final String filePerMapper) {
            this.filePerMapper = this.toLong(filePerMapper);
        }
        
        private Long toLong(final String s) {
            if (s != null) {
                return Long.valueOf(s);
            }
            return null;
        }
        
        private Integer toInteger(final String s) {
            if (s != null) {
                return Integer.valueOf(s);
            }
            return null;
        }
        
        public String getGroupByPattern() {
            return this.groupByPattern;
        }
        
        public void setGroupByPattern(final String groupByPattern) {
            this.groupByPattern = groupByPattern;
        }
        
        public Integer getTargetSize() {
            return this.targetSize;
        }
        
        public void setTargetSize(final String targetSize) {
            this.targetSize = this.toInteger(targetSize);
        }
        
        public String getOutputCodec() {
            return this.outputCodec;
        }
        
        public void setOutputCodec(final String outputCodec) {
            this.outputCodec = outputCodec;
        }
        
        public String getS3Endpoint() {
            return this.s3Endpoint;
        }
        
        public void setS3Endpoint(final String s3Endpoint) {
            this.s3Endpoint = s3Endpoint;
        }
        
        public Boolean getDeleteOnSuccess() {
            return this.deleteOnSuccess;
        }
        
        public void setDeleteOnSuccess(final Boolean deleteOnSuccess) {
            this.deleteOnSuccess = deleteOnSuccess;
        }
        
        public Boolean getDisableMultipartUpload() {
            return this.disableMultipartUpload;
        }
        
        public void setDisableMultipartUpload(final Boolean disableMultipartUpload) {
            this.disableMultipartUpload = disableMultipartUpload;
        }
        
        public String getManifestPath() {
            return this.manifestPath;
        }
        
        public void setManifestPath(final String manifestPath) {
            this.manifestPath = manifestPath;
        }
        
        public Integer getMultipartUploadPartSize() {
            return this.multipartUploadPartSize;
        }
        
        public void setMultipartUploadPartSize(final String multipartUploadPartSize) {
            this.multipartUploadPartSize = this.toInteger(multipartUploadPartSize);
        }
        
        public Long getStartingIndex() {
            return this.startingIndex;
        }
        
        public void setStartingIndex(final String startingIndex) {
            if (startingIndex != null) {
                this.startingIndex = Long.valueOf(startingIndex);
            }
            else {
                this.startingIndex = 0L;
            }
        }
        
        public Map<String, ManifestEntry> getPreviousManifest() {
            return this.previousManifest;
        }
        
        public void setPreviousManifest(final Map<String, ManifestEntry> previousManifest) {
            this.previousManifest = previousManifest;
        }
        
        public boolean getCopyFromManifest() {
            return this.copyFromManifest;
        }
        
        public void setCopyFromManifest(final boolean copyFromManifest) {
            this.copyFromManifest = copyFromManifest;
        }
    }
}
