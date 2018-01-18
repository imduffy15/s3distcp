package com.amazon.elasticmapreduce.s3distcp;

import com.amazonaws.auth.*;
import org.apache.hadoop.conf.*;

public class ConfigurationCredentials implements AWSCredentials
{
    String awsAccessKeyId;
    String awsAWSSecretKey;
    
    String getConfigOrThrow(final Configuration configuration, final String key) {
        final String value = configuration.get(key);
        if (value == null) {
            throw new RuntimeException("Unable to retrieve Hadoop configuration for key " + key);
        }
        return value;
    }
    
    public ConfigurationCredentials(final Configuration configuration) {
        super();
        this.awsAccessKeyId = this.getConfigOrThrow(configuration, "fs.s3n.awsAccessKeyId");
        this.awsAWSSecretKey = this.getConfigOrThrow(configuration, "fs.s3n.awsSecretAccessKey");
    }
    
    public String getAWSAccessKeyId() {
        return this.awsAccessKeyId;
    }
    
    public String getAWSSecretKey() {
        return this.awsAWSSecretKey;
    }
}
