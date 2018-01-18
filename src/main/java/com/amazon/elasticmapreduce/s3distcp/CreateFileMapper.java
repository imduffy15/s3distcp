package com.amazon.elasticmapreduce.s3distcp;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.fs.*;

public class CreateFileMapper extends Mapper<LongWritable, CreateFileInfo, LongWritable, CreateFileInfo>
{
    protected Mapper.Context conf;
    
    protected void setup(final Mapper.Context context) throws IOException, InterruptedException {
        this.conf = this.conf;
    }
    
    protected void map(final LongWritable key, final CreateFileInfo value, final Mapper.Context context) throws IOException, InterruptedException {
        try {
            final FileSystem fs = FileSystem.get(new URI(value.fileName.toString()), context.getConfiguration());
            final FSDataOutputStream outputFile = fs.create(new Path(value.fileName.toString()));
            long bytesLeftToWrite = value.fileSize.get();
            final byte[] buffer = new byte[12582912];
            for (int i = 0; i < buffer.length && i < bytesLeftToWrite; ++i) {
                buffer[i] = (byte)(i % 127);
            }
            while (bytesLeftToWrite > buffer.length) {
                outputFile.write(buffer);
                bytesLeftToWrite -= buffer.length;
                context.progress();
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
