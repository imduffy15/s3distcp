package com.amazon.external.elasticmapreduce.s3distcp;

import org.apache.hadoop.io.*;

class CreateFileInfo extends WritableStruct implements Cloneable
{
    public Text fileName;
    public LongWritable fileSize;
    
    public CreateFileInfo() {
        super();
        this.fileName = new Text();
        this.fileSize = new LongWritable();
    }
    
    public CreateFileInfo(final String fileName, final long fileSize) {
        super();
        this.fileName = new Text();
        this.fileSize = new LongWritable();
        this.fileName = new Text(fileName);
        this.fileSize = new LongWritable(fileSize);
    }
    
    public CreateFileInfo clone() {
        return new CreateFileInfo(this.fileName.toString(), this.fileSize.get());
    }
    
    @Override
    public Writable[] getFields() {
        return new Writable[] { this.fileName, this.fileSize };
    }
}
