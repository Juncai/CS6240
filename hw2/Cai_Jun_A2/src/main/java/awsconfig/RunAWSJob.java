package awsconfig;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * Created by jon on 1/30/16.
 */
public class RunAWSJob {
    public static void main(String[] args) {

        // load credentials
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Can't load credentials.");
        }

        // S3
        // create a bucket
        AmazonS3Client s3 = new AmazonS3Client(credentials);
        Region us

    }
}
