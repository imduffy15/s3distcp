package com.amazon.elasticmapreduce.s3distcp;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import org.apache.hadoop.fs.*;
import java.util.regex.*;
import org.apache.commons.logging.*;

public class GroupFilesMapper extends Mapper<LongWritable, FileInfo, Text, FileInfo>
{
    private static final Log log;
    protected Configuration conf;
    protected Pattern pattern;
    private String destDir;
    
    public GroupFilesMapper() {
        super();
        this.pattern = null;
    }
    
    protected void setup(final Mapper.Context context) throws IOException, InterruptedException {
        this.conf = context.getConfiguration();
        final String patternString = this.conf.get("s3DistCp.listfiles.groupByPattern");
        if (patternString != null) {
            this.pattern = Pattern.compile(patternString);
        }
        this.destDir = this.conf.get("s3DistCp.copyfiles.destDir");
    }
    
    protected void map(final LongWritable fileUID, final FileInfo fileInfo, final Mapper.Context context) throws IOException, InterruptedException {
        String path = new Path(fileInfo.inputFileName.toString()).toUri().getPath();
        if (path.startsWith(this.destDir)) {
            path = path.substring(this.destDir.length());
        }
        Text key = new Text(path);
        if (this.pattern != null) {
            final Matcher matcher = this.pattern.matcher(fileInfo.inputFileName.toString());
            if (matcher.matches()) {
                final int numGroups = matcher.groupCount();
                final StringBuilder builder = new StringBuilder();
                for (int i = 0; i < numGroups; ++i) {
                    builder.append(matcher.group(i + 1));
                }
                key = new Text(builder.toString());
            }
        }
        GroupFilesMapper.log.debug((Object)("Adding " + key.toString() + ": " + fileInfo.inputFileName.toString()));
        context.write((Object)key, (Object)fileInfo);
    }
    
    static {
        log = LogFactory.getLog((Class)GroupFilesMapper.class);
    }
}
