package com.amazon.external.elasticmapreduce.s3distcp;

import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapred.*;
import org.apache.commons.logging.*;

public class CreateSampleData implements Tool
{
    private static final Log LOG;
    protected JobConf conf;
    
    public static void main(final String[] args) throws Exception {
        final JobConf job = new JobConf((Class)CreateSampleData.class);
        final CreateSampleData distcp = new CreateSampleData(job);
        final int result = ToolRunner.run((Tool)distcp, args);
        System.exit(result);
    }
    
    public CreateSampleData(final JobConf conf) {
        super();
        this.conf = new JobConf((Configuration)conf);
    }
    
    void createFileList(final Path inputFileListPath, final String inputLocation, final URI inputUri, final URI outputUri) throws IOException {
        final FileSystem inputFS = FileSystem.get(inputUri, (Configuration)this.conf);
        final FileSystem inputFileListFS = FileSystem.get(inputFileListPath.toUri(), (Configuration)this.conf);
        final Path inputPath = new Path(inputLocation);
        final LongWritable uid = new LongWritable(1L);
        inputFileListFS.delete(inputFileListPath, true);
        inputFileListFS.mkdirs(inputFileListPath);
        final SequenceFile.Writer fileInfoWriter = SequenceFile.createWriter(inputFileListFS, (Configuration)this.conf, inputFileListPath, (Class)LongWritable.class, (Class)FileInfo.class, SequenceFile.CompressionType.NONE);
        try {
            final FileStatus[] arr$;
            final FileStatus[] contents = arr$ = inputFS.listStatus(inputPath);
            for (final FileStatus child : arr$) {
                final String inputFilePath = child.getPath().toString();
                final String outputFilePath = this.join(outputUri.toString(), child.getPath().getName());
                final FileInfo info = new FileInfo(uid.get(), inputFilePath, outputFilePath, child.getLen());
                fileInfoWriter.append((Writable)uid, (Writable)info);
                uid.set(uid.get() + 1L);
            }
        }
        finally {
            fileInfoWriter.close();
        }
        FileStatus[] arr$;
        final FileStatus[] fileListContents = arr$ = inputFileListFS.listStatus(inputFileListPath);
        for (final FileStatus status : arr$) {
            CreateSampleData.LOG.info((Object)("fileListContents: " + status.getPath()));
        }
    }
    
    private void createInputFiles(final String inputPathString, final long numFiles, final long fileSize, final String outputPath) {
        try {
            final FileSystem fs = FileSystem.get(new URI(inputPathString), (Configuration)this.conf);
            fs.mkdirs(new Path(inputPathString));
            for (int fileNumber = 1; fileNumber <= numFiles; ++fileNumber) {
                final String inputFileName = this.join(inputPathString, Integer.valueOf(fileNumber));
                final Path inputFilePath = new Path(inputFileName);
                fs.delete(inputFilePath, true);
                final SequenceFile.Writer writer = SequenceFile.createWriter(fs, (Configuration)this.conf, inputFilePath, (Class)LongWritable.class, (Class)CreateFileInfo.class, SequenceFile.CompressionType.NONE);
                try {
                    writer.append((Writable)new LongWritable((long)fileNumber), (Writable)new CreateFileInfo(this.join(outputPath, Integer.valueOf(fileNumber)), fileSize));
                }
                finally {
                    writer.close();
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public Configuration getConf() {
        return (Configuration)this.conf;
    }
    
    private String join(final String s, final Integer t) {
        return this.join(s, t.toString());
    }
    
    private String join(final String s, final String t) {
        if (s.length() != 0 && s.charAt(s.length() - 1) == '/') {
            return s + t;
        }
        return s + "/" + t;
    }
    
    public int run(final String[] args) throws Exception {
        final String outputLocation = args[0];
        final long numFiles = this.conf.getLong("createSampleData.numFiles", 5L);
        final long fileSize = this.conf.getLong("createSampleData.fileSize", 104857600L);
        final String jobName = this.conf.get("createSampleData.baseJobName", "CreateSampleData");
        final String tmpPathString = this.conf.get("createSampleData.tmpDir", "hdfs:///tmp/createSampleData");
        final String inputPathString = this.conf.get("createSampleData.workingInputDir", this.join(tmpPathString, "input"));
        final String outputPathString = this.conf.get("createSampleData.workingOutputDir", this.join(tmpPathString, "output"));
        FileSystem.get(new URI(outputPathString), (Configuration)this.conf).delete(new Path(outputPathString), true);
        this.createInputFiles(inputPathString, numFiles, fileSize, outputLocation);
        return this.runCreateJob(inputPathString, outputPathString, jobName);
    }
    
    int runCreateJob(final String inputPathString, final String outputPathString, final String jobName) throws IOException {
        final JobConf jobConf = new JobConf((Configuration)this.conf);
        jobConf.setJobName(jobName);
        jobConf.setMapSpeculativeExecution(false);
        FileInputFormat.addInputPath(jobConf, new Path(inputPathString));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPathString));
        jobConf.setInputFormat((Class)SequenceFileInputFormat.class);
        jobConf.setOutputKeyClass((Class)LongWritable.class);
        jobConf.setOutputValueClass((Class)CreateFileInfo.class);
        jobConf.setMapperClass((Class)CreateFileMapper.class);
        jobConf.setReducerClass((Class)IdentityReducer.class);
        jobConf.setOutputFormat((Class)SequenceFileOutputFormat.class);
        final RunningJob result = JobClient.runJob(jobConf);
        return result.isSuccessful() ? 0 : -1;
    }
    
    public void setConf(final Configuration conf) {
        this.conf = new JobConf(conf);
    }
    
    static {
        LOG = LogFactory.getLog((Class)CreateSampleData.class);
    }
}
