package org.apache.hadoop.fs.common;

import java.io.*;

public interface Abortable
{
    void abort() throws IOException;
}
