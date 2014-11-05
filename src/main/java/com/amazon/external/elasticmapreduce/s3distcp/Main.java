package com.amazon.external.elasticmapreduce.s3distcp;

import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;

public class Main
{
    private static final Log log;
    
    public static void main(final String[] args) throws Exception {
        Main.log.debug((Object)("Running with args: " + args));
        System.exit(ToolRunner.run((Tool)new S3DistCp(), args));
    }
    
    static {
        log = LogFactory.getLog((Class)S3DistCp.class);
    }
}
