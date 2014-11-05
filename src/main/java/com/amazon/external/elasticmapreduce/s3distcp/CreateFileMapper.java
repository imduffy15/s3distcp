package com.amazon.external.elasticmapreduce.s3distcp;

import org.apache.hadoop.io.*;
import java.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import java.net.*;
import org.apache.hadoop.fs.*;

public class CreateFileMapper implements Mapper<LongWritable, CreateFileInfo, LongWritable, CreateFileInfo>
{
    protected JobConf conf;
    
    public void close() throws IOException {
    }
    
    public void configure(final JobConf conf) {
        this.conf = conf;
    }
    
    public void map(final LongWritable key, final CreateFileInfo value, final OutputCollector<LongWritable, CreateFileInfo> output, final Reporter reporter) throws IOException {
        try {
            final FileSystem fs = FileSystem.get(new URI(value.fileName.toString()), (Configuration)this.conf);
            final FSDataOutputStream outputFile = fs.create(new Path(value.fileName.toString()));
            long bytesLeftToWrite = value.fileSize.get();
            final byte[] buffer = new byte[12582912];
            for (int i = 0; i < buffer.length && i < bytesLeftToWrite; ++i) {
                buffer[i] = (byte)(i % 127);
            }
            while (bytesLeftToWrite > buffer.length) {
                outputFile.write(buffer);
                bytesLeftToWrite -= buffer.length;
                reporter.progress();
            }
            if (bytesLeftToWrite > 0L) {
                outputFile.write(buffer, 0, (int)bytesLeftToWrite);
                bytesLeftToWrite = 0L;
            }
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
