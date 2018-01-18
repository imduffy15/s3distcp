package com.amazon.elasticmapreduce.s3distcp;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapreduce.*;

public class CreateSampleData implements Tool
{
    private static final Log LOG;
    protected Configuration conf;
    
    public static void main(final String[] args) throws Exception {
        final CreateSampleData distcp = new CreateSampleData();
        final int result = ToolRunner.run((Tool)distcp, args);
        System.exit(result);
    }
    
    public CreateSampleData() throws IOException {
        super();
        final Configuration jobConf = this.getConf();
        this.conf = jobConf;
    }
    
    void createFileList(final Path inputFileListPath, final String inputLocation, final URI inputUri, final URI outputUri) throws IOException {
        final FileSystem inputFS = FileSystem.get(inputUri, this.conf);
        final FileSystem inputFileListFS = FileSystem.get(inputFileListPath.toUri(), this.conf);
        final Path inputPath = new Path(inputLocation);
        final LongWritable uid = new LongWritable(1L);
        inputFileListFS.delete(inputFileListPath, true);
        inputFileListFS.mkdirs(inputFileListPath);
        final SequenceFile.Writer fileInfoWriter = SequenceFile.createWriter(inputFileListFS, this.conf, inputFileListPath, (Class)LongWritable.class, (Class)FileInfo.class, SequenceFile.CompressionType.NONE);
        try {
            final FileStatus[] listStatus;
            final FileStatus[] contents = listStatus = inputFS.listStatus(inputPath);
            for (final FileStatus child : listStatus) {
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
        final FileStatus[] listStatus2;
        final FileStatus[] fileListContents = listStatus2 = inputFileListFS.listStatus(inputFileListPath);
        for (final FileStatus status : listStatus2) {
            CreateSampleData.LOG.info((Object)("fileListContents: " + status.getPath()));
        }
    }
    
    private void createInputFiles(final String inputPathString, final long numFiles, final long fileSize, final String outputPath) {
        try {
            final FileSystem fs = FileSystem.get(new URI(inputPathString), this.conf);
            fs.mkdirs(new Path(inputPathString));
            for (int fileNumber = 1; fileNumber <= numFiles; ++fileNumber) {
                final String inputFileName = this.join(inputPathString, Integer.valueOf(fileNumber));
                final Path inputFilePath = new Path(inputFileName);
                fs.delete(inputFilePath, true);
                final SequenceFile.Writer writer = SequenceFile.createWriter(fs, this.conf, inputFilePath, (Class)LongWritable.class, (Class)CreateFileInfo.class, SequenceFile.CompressionType.NONE);
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
        return this.conf;
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
        FileSystem.get(new URI(outputPathString), this.conf).delete(new Path(outputPathString), true);
        this.createInputFiles(inputPathString, numFiles, fileSize, outputLocation);
        return this.runCreateJob(inputPathString, outputPathString, jobName);
    }
    
    int runCreateJob(final String inputPathString, final String outputPathString, final String jobName) {
        int ret = -1;
        try {
            final Job job = Job.getInstance(this.conf);
            job.setJarByClass((Class)this.getClass());
            job.setJobName(jobName);
            job.setMapSpeculativeExecution(false);
            FileInputFormat.addInputPath(job, new Path(inputPathString));
            FileOutputFormat.setOutputPath(job, new Path(outputPathString));
            job.setInputFormatClass((Class)SequenceFileInputFormat.class);
            job.setOutputKeyClass((Class)LongWritable.class);
            job.setOutputValueClass((Class)CreateFileInfo.class);
            job.setMapperClass((Class)CreateFileMapper.class);
            job.setReducerClass((Class)CreateFileReducer.class);
            job.setOutputFormatClass((Class)SequenceFileOutputFormat.class);
            job.submit();
            ret = (job.waitForCompletion(true) ? 0 : -1);
        }
        catch (IOException e) {
            CreateSampleData.LOG.error((Object)e.getMessage(), (Throwable)e);
        }
        catch (InterruptedException e2) {
            CreateSampleData.LOG.error((Object)e2.getMessage(), (Throwable)e2);
        }
        catch (ClassNotFoundException e3) {
            CreateSampleData.LOG.error((Object)e3.getMessage(), (Throwable)e3);
        }
        return ret;
    }
    
    public void setConf(final Configuration conf) {
        this.conf = conf;
    }
    
    static {
        LOG = LogFactory.getLog((Class)CreateSampleData.class);
    }
    
    public static class CreateFileReducer extends Reducer<LongWritable, CreateFileInfo, LongWritable, CreateFileInfo>
    {
    }
}
