package com.amazon.elasticmapreduce.s3distcp;

import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;

public class Main
{
    private static final Log log;
    
    public static void main(final String[] args) throws Exception {
        String str = "";
        if (args != null && args.length > 0) {
            final StringBuilder sb = new StringBuilder();
            for (final String s : args) {
                sb.append(s).append(" ");
            }
            str = sb.toString();
        }
        Main.log.info((Object)("Running with args: " + str));
        System.exit(ToolRunner.run((Tool)new S3DistCp(), args));
    }
    
    static {
        log = LogFactory.getLog((Class)S3DistCp.class);
    }
}
