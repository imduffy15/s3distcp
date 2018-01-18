package com.amazon.elasticmapreduce.s3distcp;

import java.util.zip.*;
import com.google.gson.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.commons.logging.*;

public class ManifestIterator implements AutoCloseable
{
    private static final Log LOG;
    private Path manifestPath;
    private GZIPInputStream gzipStream;
    private Gson gson;
    private Scanner scanner;
    
    public ManifestIterator(final Path manifestPath, final Configuration conf) throws IOException {
        super();
        this.manifestPath = manifestPath;
        final FileSystem fs = FileSystem.get(manifestPath.toUri(), conf);
        final FSDataInputStream inStream = fs.open(manifestPath);
        this.gzipStream = new GZIPInputStream(new ByteCounterInputStream((InputStream)inStream, fs.getFileStatus(manifestPath).getLen()));
        this.scanner = new Scanner(this.gzipStream);
        this.gson = new Gson();
    }
    
    public boolean hasNext() {
        return this.scanner.hasNextLine();
    }
    
    public ManifestEntry getNext() throws Exception {
        if (!this.scanner.hasNextLine()) {
            throw new IOException("Manifest iterator reached the end of file");
        }
        final String line = this.scanner.nextLine();
        final ManifestEntry entry = (ManifestEntry)this.gson.fromJson(line, (Class)ManifestEntry.class);
        return entry;
    }
    
    @Override
    public void close() {
        try {
            this.gzipStream.close();
        }
        catch (IOException e) {
            ManifestIterator.LOG.warn((Object)("Failed to close stream for manifest file " + this.manifestPath), (Throwable)e);
        }
    }
    
    static {
        LOG = LogFactory.getLog((Class)ManifestIterator.class);
    }
}
