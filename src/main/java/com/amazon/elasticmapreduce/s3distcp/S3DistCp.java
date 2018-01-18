package com.amazon.elasticmapreduce.s3distcp;

import emr.hbase.options.Options;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.commons.math3.util.*;
import org.apache.hadoop.fs.*;

import java.net.URI;
import java.util.concurrent.*;
import com.amazonaws.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.auth.*;
import java.net.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.params.*;
import java.io.*;
import java.util.regex.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.commons.logging.*;
import com.amazonaws.services.s3.model.*;
import emr.hbase.options.*;
import com.google.common.base.*;
import java.util.*;

public class S3DistCp implements Tool
{
    private static final Log LOG;
    private static final int MAX_LIST_RETRIES = 10;
    public static final String EC2_META_AZ_URL = "http://169.254.169.254/latest/meta-data/placement/availability-zone";
    public static final String S3_ENDPOINT_PDT = "s3-us-gov-west-1.amazonaws.com";
    private static String ec2MetaDataAz;
    private Configuration conf;
    
    private List<Pair<Path, Boolean>> getSrcPrefixes(final Path srcPath, final boolean srcEndWithSlash, final Path srcPrefixesFile) {
        final List<Pair<Path, Boolean>> srcPrefixes = new ArrayList<Pair<Path, Boolean>>();
        if (srcPrefixesFile == null) {
            srcPrefixes.add((Pair<Path, Boolean>)new Pair((Object)srcPath, (Object)srcEndWithSlash));
            return srcPrefixes;
        }
        try {
            final FileSystem fs = srcPrefixesFile.getFileSystem(this.conf);
            try (final InputStream is = (InputStream)fs.open(srcPrefixesFile)) {
                String srcPathStr = srcPath.toString();
                if (srcEndWithSlash && !srcPathStr.endsWith("/")) {
                    srcPathStr += "/";
                }
                final Scanner scanner = new Scanner(is);
                while (scanner.hasNextLine()) {
                    final String prefix = scanner.nextLine();
                    if (!prefix.startsWith(srcPathStr)) {
                        S3DistCp.LOG.fatal((Object)("srcPrefix: " + prefix + " does not start with src: " + srcPathStr));
                        System.exit(3);
                    }
                    else {
                        srcPrefixes.add((Pair<Path, Boolean>)new Pair((Object)new Path(prefix), (Object)prefix.endsWith("/")));
                    }
                }
            }
        }
        catch (IOException e) {
            S3DistCp.LOG.fatal((Object)("Failed to read srcPrefixesFile: " + srcPrefixesFile), (Throwable)e);
            System.exit(3);
        }
        if (srcPrefixes.size() == 0) {
            srcPrefixes.add((Pair<Path, Boolean>)new Pair((Object)srcPath, (Object)srcEndWithSlash));
        }
        return srcPrefixes;
    }
    
    public void createInputFileList(final Configuration conf, final Path srcPath, final FileInfoListing fileInfoListing, final boolean srcEndWithSlash) {
        this.createInputFileList(conf, srcPath, null, fileInfoListing, srcEndWithSlash);
    }
    
    private void createInputFileList(final Configuration conf, final Path srcPath, final Path srcPrefixesFile, final FileInfoListing fileInfoListing, final boolean srcEndWithSlash) {
        final URI srcUri = srcPath.toUri();
        if (Utils.isS3Scheme(srcUri.getScheme())) {
            final List<Pair<Path, Boolean>> srcPrefixes = this.getSrcPrefixes(srcPath, srcEndWithSlash, srcPrefixesFile);
            if (srcPrefixes.size() == 1) {
                this.createInputFileListS3(conf, ((Path)srcPrefixes.get(0).getFirst()).toUri(), fileInfoListing, (boolean)srcPrefixes.get(0).getSecond());
            }
            else {
                this.parallelizedCreateInputFileListS3(conf, srcPrefixes, fileInfoListing);
            }
        }
        else {
            try {
                final FileSystem fs = srcPath.getFileSystem(conf);
                final Queue<Path> pathsToVisit = new ArrayDeque<Path>();
                for (final Pair<Path, Boolean> prefix : this.getSrcPrefixes(srcPath, srcEndWithSlash, srcPrefixesFile)) {
                    pathsToVisit.add((Path)prefix.getFirst());
                }
                while (pathsToVisit.size() > 0) {
                    final Path curPath = pathsToVisit.remove();
                    final FileStatus[] listStatus;
                    final FileStatus[] statuses = listStatus = fs.listStatus(curPath);
                    for (final FileStatus status : listStatus) {
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
    
    private void parallelizedCreateInputFileListS3(final Configuration conf, final List<Pair<Path, Boolean>> srcPrefixes, final FileInfoListing fileInfoListing) {
        final int numThreads = conf.getInt("s3DistCp.listfiles.srcPrefixes.numThreads", 20);
        final ThreadPoolExecutor listExecutor = Utils.createExecutorService("s3distcp-list-worker", numThreads);
        final List<Future<Void>> futures = new ArrayList<Future<Void>>();
        for (final Pair<Path, Boolean> prefix : srcPrefixes) {
            final Future<Void> future = listExecutor.submit((Callable<Void>)new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    S3DistCp.this.createInputFileListS3(conf, ((Path)prefix.getFirst()).toUri(), fileInfoListing, (boolean)prefix.getSecond());
                    return null;
                }
            });
            futures.add(future);
        }
        try {
            for (final Future future2 : futures) {
                future2.get();
            }
        }
        catch (InterruptedException e) {}
        catch (ExecutionException e) {
            S3DistCp.LOG.fatal((Object)"Failed to list objects", (Throwable)e);
            throw new RuntimeException(e);
        }
        finally {
            try {
                listExecutor.shutdown();
            }
            catch (Exception e2) {
                S3DistCp.LOG.warn((Object)"Error shutdown executor", (Throwable)e2);
            }
        }
    }
    
    public void createInputFileListS3(final Configuration conf, final URI srcUri, final FileInfoListing fileInfoListing, final boolean srcEndWithSlash) {
        final AmazonS3Client s3Client = createAmazonS3Client(conf);
        ObjectListing objects = null;
        boolean finished = false;
        int retryCount = 0;
        final String scheme = (srcUri.getScheme() == null) ? "s3://" : (srcUri.getScheme() + "://");
        while (!finished) {
            final ListObjectsRequest listObjectRequest = new ListObjectsRequest().withBucketName(Utils.uriToBucket(srcUri));
            if (srcUri.getPath().length() > 1) {
                String prefix = srcUri.getPath().substring(1);
                if (srcEndWithSlash && !prefix.endsWith("/")) {
                    prefix += "/";
                }
                listObjectRequest.setPrefix(prefix);
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
            final List<Pair<Path, Long>> s3Files = new ArrayList<Pair<Path, Long>>();
            for (final S3ObjectSummary object : objects.getObjectSummaries()) {
                if (object.getKey().endsWith("/")) {
                    S3DistCp.LOG.info((Object)("Skipping key '" + object.getKey() + "' because it ends with '/'"));
                }
                else {
                    final String s3FilePath = scheme + object.getBucketName() + "/" + object.getKey();
                    s3Files.add((Pair<Path, Long>)new Pair((Object)new Path(s3FilePath), (Object)object.getSize()));
                }
            }
            synchronized (this) {
                for (final Pair<Path, Long> s3File : s3Files) {
                    S3DistCp.LOG.debug((Object)("About to add " + s3File.getFirst()));
                    fileInfoListing.add((Path)s3File.getFirst(), (long)s3File.getSecond());
                }
            }
            if (!objects.isTruncated()) {
                finished = true;
            }
        }
    }
    
    public static AmazonS3Client createAmazonS3Client(final Configuration conf) {
        final String accessKeyId = conf.get("fs.s3n.awsAccessKeyId");
        final String SecretAccessKey = conf.get("fs.s3n.awsSecretAccessKey");
        AmazonS3Client s3Client;
        if (accessKeyId != null && SecretAccessKey != null) {
            s3Client = new AmazonS3Client((AWSCredentials)new BasicAWSCredentials(accessKeyId, SecretAccessKey));
            S3DistCp.LOG.info((Object)("Hadoop Configuration is used to create AmazonS3Client. KeyId: " + accessKeyId));
        }
        else {
            final AWSCredentialsProvider provider = (AWSCredentialsProvider)new DefaultAWSCredentialsProviderChain();
            s3Client = new AmazonS3Client(provider);
            S3DistCp.LOG.info((Object)("DefaultAWSCredentialsProviderChain is used to create AmazonS3Client. KeyId: " + provider.getCredentials().getAWSAccessKeyId()));
        }
        String endpoint = conf.get("fs.s3n.endpoint");
        if (endpoint == null && isGovCloud()) {
            endpoint = "s3-us-gov-west-1.amazonaws.com";
        }
        if (endpoint != null) {
            S3DistCp.LOG.info((Object)("AmazonS3Client setEndpoint " + endpoint));
            s3Client.setEndpoint(endpoint);
        }
        return s3Client;
    }
    
    private static String getHostName() {
        try {
            final InetAddress addr = InetAddress.getLocalHost();
            return addr.getHostName();
        }
        catch (UnknownHostException ex) {
            return "unknown";
        }
    }
    
    private static boolean isGovCloud() {
        if (S3DistCp.ec2MetaDataAz != null) {
            return S3DistCp.ec2MetaDataAz.startsWith("us-gov-west-1");
        }
        final String hostname = getHostName();
        final int timeout = hostname.startsWith("ip-") ? 30000 : 1000;
        final GetMethod getMethod = new GetMethod("http://169.254.169.254/latest/meta-data/placement/availability-zone");
        try {
            final HttpConnectionManager manager = (HttpConnectionManager)new SimpleHttpConnectionManager();
            final HttpConnectionManagerParams params = manager.getParams();
            params.setConnectionTimeout(timeout);
            params.setSoTimeout(timeout);
            final HttpClient httpClient = new HttpClient(manager);
            final int status = httpClient.executeMethod(getMethod);
            if (status >= 200 && status <= 299) {
                S3DistCp.ec2MetaDataAz = getMethod.getResponseBodyAsString().trim();
                S3DistCp.LOG.info((Object)("GET http://169.254.169.254/latest/meta-data/placement/availability-zone result: " + S3DistCp.ec2MetaDataAz));
                return S3DistCp.ec2MetaDataAz.startsWith("us-gov-west-1");
            }
            S3DistCp.LOG.info((Object)("error status code" + status + " GET " + "http://169.254.169.254/latest/meta-data/placement/availability-zone"));
        }
        catch (Exception e) {
            S3DistCp.LOG.info((Object)("GET http://169.254.169.254/latest/meta-data/placement/availability-zone exception: " + e.getMessage()));
        }
        finally {
            getMethod.releaseConnection();
        }
        return false;
    }
    
    public int run(final String[] args) throws IllegalArgumentException, IOException {
        if (args != null && args.length > 0) {
            final StringBuilder sb = new StringBuilder();
            for (final String arg : args) {
                sb.append(arg).append(" ");
            }
            S3DistCp.LOG.info((Object)("S3DistCp args: " + sb.toString()));
        }
        final S3DistCpOptions options = new S3DistCpOptions(args, this.conf);
        if (options.isHelpDefined()) {
            return 0;
        }
        return this.run(options);
    }
    
    public int run(final S3DistCpOptions options) {
        final Configuration jobConf = this.getConf();
        Path srcPath = new Path(options.getSrcPath());
        if (!srcPath.isAbsolute()) {
            S3DistCp.LOG.fatal((Object)"Source path must be absolute");
            System.exit(5);
        }
        final String tempDirRoot = "hdfs:///tmp/" + UUID.randomUUID();
        final Path outputPath = new Path(tempDirRoot, "output");
        final Path inputPath = new Path(tempDirRoot, "files");
        final Path tempPath = new Path(tempDirRoot, "tempspace");
        final Path destPath = new Path(options.getDest());
        if (!destPath.isAbsolute()) {
            S3DistCp.LOG.fatal((Object)"Destination path must be absolute");
            System.exit(4);
        }
        S3DistCp.LOG.info((Object)("Using output path '" + outputPath.toString() + "'"));
        jobConf.set("s3DistCp.copyfiles.destDir", destPath.toString());
        jobConf.setBoolean("s3DistCp.copyfiles.reducer.numberFiles", (boolean)options.getNumberFiles());
        if (options.getS3ServerSideEncryption() != null) {
            jobConf.setBoolean("fs.s3.enableServerSideEncryption", (boolean)options.getS3ServerSideEncryption());
        }
        File manifestFile = null;
        if (options.getManifestPath() != null) {
            manifestFile = new File(options.getManifestPath());
        }
        jobConf.setBoolean("s3distcp.copyFiles.appendToLastFile", options.getAppendToLastFileOption());
        if (options.getS3Endpoint() != null) {
            jobConf.set("fs.s3n.endpoint", options.getS3Endpoint());
        }
        else if (isGovCloud()) {
            jobConf.set("fs.s3n.endpoint", "s3-us-gov-west-1.amazonaws.com");
        }
        if (options.getDisableMultipartUpload() != null) {
            jobConf.setBoolean("fs.s3n.multipart.uploads.enabled", !options.getDisableMultipartUpload());
        }
        if (options.getMultipartUploadPartSize() != null) {
            final int partSize = options.getMultipartUploadPartSize();
            if (partSize < 5) {
                S3DistCp.LOG.fatal((Object)("Multipart upload part size of " + partSize + " MiB is too small."));
                return 2;
            }
            if (partSize > 5120) {
                S3DistCp.LOG.fatal((Object)("Multipart upload part size of " + partSize + " MiB is too large."));
                return 2;
            }
            jobConf.setInt("fs.s3n.multipart.uploads.split.size", options.getMultipartUploadPartSize() * 1024 * 1024);
        }
        if (options.getTargetSize() != null) {
            try {
                final long targetSize = options.getTargetSize();
                jobConf.setLong("s3DistCp.copyfiles.reducer.targetSize", targetSize * 1024L * 1024L);
            }
            catch (Exception e6) {
                System.err.println("Error parsing target file size");
                return 2;
            }
        }
        final String outputCodec = options.getOutputCodec();
        S3DistCp.LOG.debug((Object)("outputCodec: " + outputCodec));
        jobConf.set("s3DistCp.copyfiles.reducer.outputCodec", outputCodec);
        jobConf.setBoolean("s3DistCp.copyFiles.deleteFilesOnSuccess", (boolean)options.getDeleteOnSuccess());
        if (options.getStorageClass() != null) {
            jobConf.set("fs.s3.storageClass", options.getStorageClass().toString());
        }
        this.deleteRecursive(jobConf, inputPath);
        this.deleteRecursive(jobConf, outputPath);
        try {
            final FileSystem fs = FileSystem.get(srcPath.toUri(), jobConf);
            srcPath = fs.getFileStatus(srcPath).getPath();
        }
        catch (Exception e) {
            S3DistCp.LOG.fatal((Object)"Failed to get source file system", (Throwable)e);
            throw new RuntimeException("Failed to get source file system", e);
        }
        jobConf.set("s3DistCp.copyfiles.srcDir", srcPath.toString());
        FileInfoListing fileInfoListing = null;
        try {
            Map<String, ManifestEntry> previousManifest = null;
            if (!options.copyFromManifest) {
                previousManifest = options.getPreviousManifest();
            }
            fileInfoListing = new FileInfoListing(jobConf, srcPath, inputPath, destPath, options.getStartingIndex(), manifestFile, previousManifest);
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
                jobConf.set("s3DistCp.listfiles.groupByPattern", groupByPattern);
            }
            catch (Exception e7) {
                System.err.println("Invalid group by pattern");
                System.exit(1);
            }
        }
        if (options.getFilePerMapper() != null) {
            fileInfoListing.setRecordsPerFile(options.getFilePerMapper());
        }
        try {
            if (options.isCopyFromManifest() && (options.getPreviousManifest() != null || options.getPreviousManifestPath() != null)) {
                if (options.getPreviousManifest() != null) {
                    fileInfoListing = this.addFilesFromLoadedPrevManifest(options.getPreviousManifest(), fileInfoListing);
                }
                else {
                    fileInfoListing = this.addFilesFromPrevManifest(new Path(options.getPreviousManifestPath()), jobConf, fileInfoListing);
                }
            }
            else {
                final boolean srcEndWithSlash = options.getSrcPath() != null && options.getSrcPath().endsWith("/");
                this.createInputFileList(jobConf, srcPath, options.getSrcPrefixesFile(), fileInfoListing, srcEndWithSlash);
            }
            S3DistCp.LOG.info((Object)("Created " + fileInfoListing.getFileIndex() + " files to copy " + fileInfoListing.getRecordIndex() + " files "));
        }
        finally {
            fileInfoListing.close();
        }
        try {
            final FileSystem fs2 = FileSystem.get(destPath.toUri(), jobConf);
            fs2.mkdirs(destPath);
        }
        catch (IOException e3) {
            S3DistCp.LOG.fatal((Object)("Failed to create destination path " + destPath), (Throwable)e3);
            return 7;
        }
        try {
            final Job job = Job.getInstance(jobConf);
            job.setJarByClass((Class)this.getClass());
            job.setJobName("S3DistCp: " + srcPath.toString() + " -> " + destPath.toString());
            job.setReduceSpeculativeExecution(false);
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setInputFormatClass((Class)SequenceFileInputFormat.class);
            job.setOutputKeyClass((Class)Text.class);
            job.setOutputValueClass((Class)FileInfo.class);
            job.setMapperClass((Class)GroupFilesMapper.class);
            job.setReducerClass((Class)CopyFilesReducer.class);
            job.setOutputFormatClass((Class)TextOutputFormat.class);
            int reducerNum = jobConf.getInt("mapreduce.job.reduces", 10);
            if (reducerNum <= 1) {
                reducerNum = 10;
            }
            S3DistCp.LOG.info((Object)("Reducer number: " + reducerNum));
            job.setNumReduceTasks(reducerNum);
            job.submit();
            final boolean jobSucceeded = job.waitForCompletion(true);
            this.deleteRecursiveNoThrow(jobConf, tempPath);
            final long reduceOutputRecords = job.getCounters().findCounter((Enum)TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
            if (reduceOutputRecords > 0L) {
                S3DistCp.LOG.error((Object)(reduceOutputRecords + " files failed to copy"));
                throw new RuntimeException(reduceOutputRecords + " files failed to copy");
            }
            final FileSystem tempFs = FileSystem.get(tempPath.toUri(), jobConf);
            tempFs.delete(tempPath, true);
            if (!jobSucceeded) {
                final String errorMessage = "The MapReduce job failed: " + job.getStatus().getFailureInfo();
                S3DistCp.LOG.error((Object)errorMessage);
                throw new RuntimeException(errorMessage);
            }
            if (manifestFile != null) {
                final FileSystem destFs = FileSystem.get(destPath.toUri(), jobConf);
                destFs.copyFromLocalFile(new Path(manifestFile.getAbsolutePath()), destPath);
                manifestFile.delete();
            }
        }
        catch (IOException e3) {
            this.deleteRecursiveNoThrow(jobConf, tempPath);
            throw new RuntimeException("Error running job", e3);
        }
        catch (InterruptedException e4) {
            this.deleteRecursiveNoThrow(jobConf, tempPath);
            S3DistCp.LOG.warn((Object)e4.getMessage(), (Throwable)e4);
        }
        catch (ClassNotFoundException e5) {
            S3DistCp.LOG.warn((Object)e5.getMessage(), (Throwable)e5);
        }
        return 0;
    }
    
    private FileInfoListing addFilesFromLoadedPrevManifest(final Map<String, ManifestEntry> previousManifest, final FileInfoListing fileInfoListing) {
        for (final ManifestEntry entry : previousManifest.values()) {
            fileInfoListing.add(new Path(entry.path), new Path(entry.srcDir), entry.size);
        }
        return fileInfoListing;
    }
    
    private FileInfoListing addFilesFromPrevManifest(final Path manifestPath, final Configuration config, final FileInfoListing fileInfoListing) {
        try (final ManifestIterator manifestIterator = new ManifestIterator(manifestPath, this.conf)) {
            while (manifestIterator.hasNext()) {
                final ManifestEntry entry = manifestIterator.getNext();
                fileInfoListing.add(new Path(entry.path), new Path(entry.srcDir), entry.size);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to add file info from previous manifest file '" + manifestPath + "'", e);
        }
        return fileInfoListing;
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
        S3DistCp.ec2MetaDataAz = null;
    }
    
    public static class S3DistCpOptions
    {
        private static final Log LOG;
        private String srcPath;
        private String dest;
        private boolean numberFiles;
        private boolean s3ServerSideEncryption;
        private String srcPattern;
        private Long filePerMapper;
        private String groupByPattern;
        private Integer targetSize;
        private boolean appendToLastFile;
        private String outputCodec;
        private String s3Endpoint;
        private boolean deleteOnSuccess;
        private boolean disableMultipartUpload;
        private boolean requirePreviousManifest;
        private String manifestPath;
        private Integer multipartUploadPartSize;
        private Long startingIndex;
        private Map<String, ManifestEntry> previousManifest;
        private String previousManifestPath;
        private Boolean copyFromManifest;
        private boolean helpDefined;
        private StorageClass storageClass;
        private Path srcPrefixesFile;
        
        public S3DistCpOptions() {
            super();
            this.numberFiles = false;
            this.s3ServerSideEncryption = false;
            this.appendToLastFile = false;
            this.outputCodec = "keep";
            this.deleteOnSuccess = false;
            this.disableMultipartUpload = false;
            this.requirePreviousManifest = true;
            this.startingIndex = 0L;
            this.copyFromManifest = false;
            this.helpDefined = false;
            this.storageClass = null;
            this.srcPrefixesFile = null;
        }
        
        public S3DistCpOptions(final String[] args, final Configuration conf) throws IllegalArgumentException, IOException {
            super();
            this.numberFiles = false;
            this.s3ServerSideEncryption = false;
            this.appendToLastFile = false;
            this.outputCodec = "keep";
            this.deleteOnSuccess = false;
            this.disableMultipartUpload = false;
            this.requirePreviousManifest = true;
            this.startingIndex = 0L;
            this.copyFromManifest = false;
            this.helpDefined = false;
            this.storageClass = null;
            this.srcPrefixesFile = null;
            final Options options = new Options();
            final SimpleOption helpOption = options.noArg("--help", "Print help text");
            final OptionWithArg srcOption = options.withArg("--src", "Directory to copy files from");
            final OptionWithArg destOption = options.withArg("--dest", "Directory to copy files to");
            final OptionWithArg srcPatternOption = options.withArg("--srcPattern", "Include only source files matching this pattern");
            final OptionWithArg filePerMapperOption = options.withArg("--filesPerMapper", "Place up to this number of files in each map task");
            final OptionWithArg groupByPatternOption = options.withArg("--groupBy", "Pattern to group input files by");
            final OptionWithArg appendToLastFileOption = options.withArg("--appendToLastFile", "Allows appending to the last file that was previously copied when using groupBy");
            final OptionWithArg targetSizeOption = options.withArg("--targetSize", "Target size for output files");
            final OptionWithArg outputCodecOption = options.withArg("--outputCodec", "Compression codec for output files");
            final OptionWithArg s3EndpointOption = options.withArg("--s3Endpoint", "S3 endpoint to use for uploading files");
            final SimpleOption deleteOnSuccessOption = options.noArg("--deleteOnSuccess", "Delete input files after a successful copy");
            final SimpleOption disableMultipartUploadOption = options.noArg("--disableMultipartUpload", "Disable the use of multipart upload");
            final OptionWithArg multipartUploadPartSizeOption = options.withArg("--multipartUploadChunkSize", "The size in MiB of the multipart upload part size");
            final OptionWithArg startingIndexOption = options.withArg("--startingIndex", "The index to start with for file numbering");
            final SimpleOption numberFilesOption = options.noArg("--numberFiles", "Prepend sequential numbers the file names");
            final SimpleOption s3ServerSideEncryptionOption = options.noArg("--s3ServerSideEncryption", "Copy files to S3 using Amazon S3 Server Side Encryption");
            final OptionWithArg outputManifest = options.withArg("--outputManifest", "The name of the manifest file");
            final OptionWithArg requirePreviousManifestOption = options.withArg("--requirePreviousManifest", "Require that a previous manifest is present if specified");
            final OptionWithArg previousManifest = options.withArg("--previousManifest", "The path to an existing manifest file");
            final SimpleOption copyFromManifest = options.noArg("--copyFromManifest", "Copy from a manifest instead of listing a directory");
            final OptionWithArg storageClassOption = options.withArg("--storageClass", "STANDARD/STANDARD_IA/REDUCED_REDUNDANCY");
            final OptionWithArg srcPrefixesFile = options.withArg("--srcPrefixesFile", "File containing a list of source URI prefixes");
            options.parseArguments(args);
            if (helpOption.defined()) {
                S3DistCpOptions.LOG.info((Object)options.helpText());
                this.helpDefined = true;
            }
            srcOption.require();
            destOption.require();
            if (srcPrefixesFile.defined()) {
                this.setSrcPrefixesFile(srcPrefixesFile.value);
            }
            if (storageClassOption.defined()) {
                this.setStorageClass(storageClassOption.value);
            }
            if (srcOption.defined()) {
                this.setSrcPath(srcOption.value);
            }
            if (destOption.defined()) {
                this.setDest(destOption.value);
            }
            if (numberFilesOption.defined()) {
                this.setNumberFiles(numberFilesOption.value);
            }
            if (s3ServerSideEncryptionOption.defined()) {
                this.setS3ServerSideEncryption(s3ServerSideEncryptionOption.value);
            }
            if (srcPatternOption.defined()) {
                this.setSrcPattern(srcPatternOption.value);
            }
            if (filePerMapperOption.defined()) {
                this.setFilePerMapper(filePerMapperOption.value);
            }
            final String destScheme = new Path(this.getDest()).toUri().getScheme();
            if (groupByPatternOption.defined()) {
                this.setGroupByPattern(groupByPatternOption.value);
                if (!Utils.isS3Scheme(destScheme)) {
                    this.appendToLastFile = true;
                }
            }
            if (appendToLastFileOption.defined()) {
                if (appendToLastFileOption.value.toLowerCase().contains("true") && Utils.isS3Scheme(destScheme)) {
                    throw new IllegalArgumentException("--appendToLastFile option is not supported for s3");
                }
                this.setAppendToLastFileOption(appendToLastFileOption.value);
            }
            if (targetSizeOption.defined()) {
                this.setTargetSize(targetSizeOption.value);
            }
            if (outputCodecOption.defined()) {
                this.setOutputCodec(outputCodecOption.value);
            }
            if (s3EndpointOption.defined()) {
                this.setS3Endpoint(s3EndpointOption.value);
            }
            if (deleteOnSuccessOption.defined()) {
                this.setDeleteOnSuccess(deleteOnSuccessOption.value);
            }
            if (disableMultipartUploadOption.defined()) {
                this.setDisableMultipartUpload(disableMultipartUploadOption.value);
            }
            if (multipartUploadPartSizeOption.defined()) {
                this.setMultipartUploadPartSize(multipartUploadPartSizeOption.value);
            }
            if (startingIndexOption.defined()) {
                this.setStartingIndex(startingIndexOption.value);
            }
            if (numberFilesOption.defined()) {
                this.setNumberFiles(numberFilesOption.value);
            }
            if (outputManifest.defined()) {
                this.setManifestPath(outputManifest.value);
            }
            if (requirePreviousManifestOption.defined()) {
                this.setRequirePreviousManifest(requirePreviousManifestOption.value);
            }
            if (previousManifest.defined()) {
                this.previousManifestPath = previousManifest.value;
            }
            if (copyFromManifest.defined()) {
                this.setCopyFromManifest(true);
            }
            if (previousManifest.defined() && !copyFromManifest.defined()) {
                try {
                    this.setPreviousManifest(loadManifest(new Path(previousManifest.value), conf));
                }
                catch (RuntimeException e) {
                    if (this.requirePreviousManifest) {
                        throw e;
                    }
                }
            }
        }
        
        public Path getSrcPrefixesFile() {
            return this.srcPrefixesFile;
        }
        
        public void setSrcPrefixesFile(final String path) {
            this.srcPrefixesFile = new Path(path);
        }
        
        public StorageClass getStorageClass() {
            return this.storageClass;
        }
        
        public void setStorageClass(final String storageClass) {
            if (Strings.isNullOrEmpty(storageClass)) {
                this.storageClass = null;
            }
            this.storageClass = StorageClass.fromValue(storageClass);
        }
        
        public String getSrcPath() {
            return this.srcPath;
        }
        
        public void setSrcPath(final String srcPath) {
            this.srcPath = srcPath;
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
        
        public Boolean getS3ServerSideEncryption() {
            return this.s3ServerSideEncryption;
        }
        
        public void setS3ServerSideEncryption(final Boolean s3ServerSideEncryption) {
            this.s3ServerSideEncryption = s3ServerSideEncryption;
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
        
        public boolean getAppendToLastFileOption() {
            return this.appendToLastFile;
        }
        
        public void setAppendToLastFileOption(final String appendToLastFile) {
            this.appendToLastFile = Boolean.valueOf(appendToLastFile);
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
        
        public void setRequirePreviousManifest(final String requirePreviousManifest) {
            this.requirePreviousManifest = Boolean.valueOf(requirePreviousManifest);
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
        
        public String getPreviousManifestPath() {
            return this.previousManifestPath;
        }
        
        public boolean isCopyFromManifest() {
            return this.copyFromManifest;
        }
        
        public void setCopyFromManifest(final boolean copyFromManifest) {
            this.copyFromManifest = copyFromManifest;
        }
        
        public Map<String, ManifestEntry> getPreviousManifest() {
            return this.previousManifest;
        }
        
        public void setPreviousManifest(final Map<String, ManifestEntry> previousManifest) {
            this.previousManifest = previousManifest;
        }
        
        public boolean isHelpDefined() {
            return this.helpDefined;
        }
        
        public static Map<String, ManifestEntry> loadManifest(final Path manifestPath, final Configuration conf) {
            final Map<String, ManifestEntry> manifest = new TreeMap<String, ManifestEntry>();
            try (final ManifestIterator manifestIterator = new ManifestIterator(manifestPath, conf)) {
                while (manifestIterator.hasNext()) {
                    final ManifestEntry entry = manifestIterator.getNext();
                    manifest.put(entry.baseName, entry);
                }
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to load manifest file '" + manifestPath + "'", e);
            }
            return manifest;
        }
        
        static {
            LOG = LogFactory.getLog((Class)S3DistCpOptions.class);
        }
    }
}
