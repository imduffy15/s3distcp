package com.amazon.external.elasticmapreduce.s3distcp;

import java.security.*;
import java.util.concurrent.*;

public class Utils
{
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
        if (getSuffix(name).equals("")) {
            return name + suffix;
        }
        final int index = name.lastIndexOf(46);
        return name.substring(0, index) + suffix;
    }
    
    public static boolean isS3Scheme(final String scheme) {
        return scheme.equals("s3") || scheme.equals("s3n");
    }
    
    public static ThreadPoolExecutor createDefaultExecutorService() {
        final ThreadFactory threadFactory = new ThreadFactory() {
            private int threadCount = 1;
            
            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = new Thread(r);
                thread.setName("s3-transfer-manager-worker-" + this.threadCount++);
                return thread;
            }
        };
        return (ThreadPoolExecutor)Executors.newFixedThreadPool(10, threadFactory);
    }
}
