package com.amazon.elasticmapreduce.s3distcp;

public class ManifestEntry
{
    public String path;
    public String baseName;
    public String srcDir;
    public long size;
    
    public ManifestEntry() {
        super();
    }
    
    public ManifestEntry(final String path, final String baseName, final String srcDir, final long size) {
        super();
        this.path = path;
        this.baseName = baseName;
        this.srcDir = srcDir;
        this.size = size;
    }
}
