package com.amazon.external.elasticmapreduce.s3distcp;

import org.apache.hadoop.io.*;
import java.io.*;

abstract class WritableStruct implements Writable
{
    public abstract Writable[] getFields();
    
    public void readFields(final DataInput input) throws IOException {
        for (final Writable field : this.getFields()) {
            field.readFields(input);
        }
    }
    
    public void write(final DataOutput output) throws IOException {
        for (final Writable field : this.getFields()) {
            field.write(output);
        }
    }
}
