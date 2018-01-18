package com.amazon.elasticmapreduce.s3distcp;

import java.security.*;
import com.google.common.base.*;
import java.util.concurrent.*;
import java.net.*;
import org.apache.commons.logging.*;

public class Utils
{
    private static final Log LOG;
    
    public static String randomString(long value) {
        final StringBuffer result = new StringBuffer();
        if (value < 0L) {
            value = -value;
        }
        do {
            final long remainder = value % 58L;
            int c;
            if (remainder < 24L) {
                c = 'a' + (char)remainder;
            }
            else if (remainder < 48L) {
                c = 'A' + (char)(remainder - 24L);
            }
            else {
                c = '0' + (char)(remainder - 48L);
            }
            result.appendCodePoint(c);
            value /= 58L;
        } while (value > 0L);
        return result.reverse().toString();
    }
    
    public static String randomString() {
        return randomString(new SecureRandom().nextLong());
    }
    
    public static String getSuffix(final String name) {
        if (name != null) {
            final String[] parts = name.split("\\.");
            if (parts.length > 1) {
                return parts[parts.length - 1];
            }
        }
        return "";
    }
    
    public static String replaceSuffix(final String name, final String suffix) {
        final int index = name.lastIndexOf(46);
        return name.substring(0, index) + suffix;
    }
    
    public static String appendSuffix(final String name, final String suffix) {
        return name + suffix;
    }
    
    public static boolean isS3Scheme(final String scheme) {
        return "s3".equals(scheme) || "s3n".equals(scheme);
    }
    
    public static String cleanupColonsAndSlashes(String s) {
        if (Strings.isNullOrEmpty(s)) {
            return s;
        }
        s = s.replaceAll(":", "/");
        return Joiner.on("/").skipNulls().join(Splitter.on("/").trimResults().omitEmptyStrings().split((CharSequence)s));
    }
    
    public static ThreadPoolExecutor createExecutorService(final String threadName, final int numWorkers) {
        final ThreadFactory threadFactory = new ThreadFactory() {
            private int threadCount = 1;
            
            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = new Thread(r);
                thread.setName(threadName + "-" + this.threadCount++);
                thread.setDaemon(true);
                return thread;
            }
        };
        Utils.LOG.info((Object)("Created executor service " + threadName + " with " + numWorkers + " worker threads"));
        final ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor)Executors.newFixedThreadPool(numWorkers, threadFactory);
        return threadPoolExecutor;
    }
    
    public static String uriToBucket(final URI uri) {
        return uri.getAuthority();
    }
    
    static {
        LOG = LogFactory.getLog((Class)Utils.class);
    }
}
