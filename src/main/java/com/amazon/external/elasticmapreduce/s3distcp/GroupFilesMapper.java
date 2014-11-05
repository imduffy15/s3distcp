package com.amazon.external.elasticmapreduce.s3distcp;

import org.apache.hadoop.io.*;
import java.io.*;
import org.apache.hadoop.mapred.*;
import java.net.*;
import java.util.regex.*;
import org.apache.commons.logging.*;

public class GroupFilesMapper implements Mapper<LongWritable, FileInfo, Text, FileInfo>
{
    private static final Log log;
    protected JobConf conf;
    protected Pattern pattern;
    private String destDir;
    
    public GroupFilesMapper() {
        super();
        this.pattern = null;
    }
    
    public void configure(final JobConf conf) {
        this.conf = conf;
        final String patternString = conf.get("s3DistCp.listfiles.gropubypattern");
        if (patternString != null) {
            this.pattern = Pattern.compile(patternString);
        }
        this.destDir = conf.get("s3DistCp.copyfiles.destDir");
    }
    
    public void close() throws IOException {
    }
    
    public void map(final LongWritable fileUID, final FileInfo fileInfo, final OutputCollector<Text, FileInfo> collector, final Reporter reporter) throws IOException {
        Text key;
        try {
            String path = new URI(fileInfo.inputFileName.toString()).getPath();
            if (path.startsWith(this.destDir)) {
                path = path.substring(this.destDir.length());
            }
            key = new Text(path);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException("Bad URI: " + fileInfo.inputFileName.toString(), e);
        }
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
        collector.collect(key, fileInfo);
    }
    
    static {
        log = LogFactory.getLog((Class)GroupFilesMapper.class);
    }
}
