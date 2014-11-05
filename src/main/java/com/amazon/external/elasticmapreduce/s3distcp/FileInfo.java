package com.amazon.external.elasticmapreduce.s3distcp;

import org.apache.hadoop.io.*;

class FileInfo extends WritableStruct implements Cloneable
{
    public LongWritable fileUID;
    public Text inputFileName;
    public Text outputFileName;
    public LongWritable fileSize;
    
    public FileInfo() {
        super();
        this.fileUID = new LongWritable(0L);
        this.inputFileName = new Text();
        this.outputFileName = new Text();
        this.fileSize = new LongWritable(0L);
    }
    
    public FileInfo(final Long fileUID, final String inputUri, final String outputUri, final long fileSize) {
        super();
        this.fileUID = new LongWritable(0L);
        this.inputFileName = new Text();
        this.outputFileName = new Text();
        this.fileSize = new LongWritable(0L);
        this.fileUID = new LongWritable((long)fileUID);
        this.inputFileName = new Text(inputUri);
        this.outputFileName = new Text(outputUri);
        this.fileSize = new LongWritable(fileSize);
    }
    
    public FileInfo clone() {
        return new FileInfo(this.fileUID.get(), this.inputFileName.toString(), this.outputFileName.toString(), this.fileSize.get());
    }
    
    @Override
    public Writable[] getFields() {
        return new Writable[] { this.fileUID, this.inputFileName, this.outputFileName, this.fileSize };
    }
    
    @Override
    public String toString() {
        return "{" + this.fileUID + ", '" + this.inputFileName + "', '" + this.outputFileName + "', " + this.fileSize + "}";
    }
}
