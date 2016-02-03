package awsconfig;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.identitymanagement.model.InstanceProfile;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * Created by jon on 1/30/16.
 * Code reference: https://javatutorial.net/java-s3-example
 */
public class RunAWSJob {
    private final static String SUFFIX = "/";
    private final static String INPUT = "MR_INPUT_PATH";
    private final static String BUCKET_NAME = "MR_BUCKET";
    private final static String BUCKET_INPUT = "input";
    private final static String BUCKET_OUTPUT = "output";
    private final static String OUTPUT = "output";
    private final static String MAIN_CLASS = "ClusterAnalysis";
    private final static String S3_PREFIX = "s3://";
    private final static String S3_INPUT = S3_PREFIX + BUCKET_NAME + BUCKET_INPUT;
    private final static String S3_OUTPUT = S3_PREFIX + BUCKET_NAME + BUCKET_OUTPUT;

    public static void main(String[] args) {
        // load environment variables
        String inputFolder = System.getenv(INPUT);
        String bucketName = System.getenv(BUCKET_NAME);

        // load credentials
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Can't load credentials.");
        }

        // S3 jar and data input
        // create a bucket
        AmazonS3Client s3 = new AmazonS3Client(credentials);

        // upload jar file
        String jarName = "analysis.jar";
        File f = new File("build/libs/analysis.jar");
        s3.createBucket(bucketName);
        s3.putObject(new PutObjectRequest(bucketName, jarName, f));

        // create input folder and upload data files
        createFolder(bucketName, BUCKET_INPUT, s3);
        uploadDirectory(bucketName, inputFolder, BUCKET_INPUT, s3);

        // EMR run job
        AmazonElasticMapReduce client = new AmazonElasticMapReduceClient(credentials);
        HadoopJarStepConfig hadoopConfig = new HadoopJarStepConfig()
                .withJar(S3_PREFIX + bucketName + SUFFIX + jarName)
                .withMainClass(MAIN_CLASS)
                .withArgs(S3_INPUT, S3_OUTPUT);
        StepConfig customStep = new StepConfig("ClusterAnalysisStep", hadoopConfig);
//        InstanceProfile profile = new InstanceProfile()
        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("Cluster Name")
//                .withReleaseLabel("emr-4.3.0")
                .withSteps(customStep)
                .withLogUri("s3://bucket/logs")
//                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withInstances(new JobFlowInstancesConfig().withKeepJobFlowAliveWhenNoSteps(true)
                .withEc2KeyName("jon_ec2_key")
                .withInstanceCount(3)
                .withKeepJobFlowAliveWhenNoSteps(true)
                .withMasterInstanceType("m3.xlarge")
                .withSlaveInstanceType("m3.xlarge"));
        RunJobFlowResult result = client.runJobFlow(request);
        System.out.println("jobFlowId: " + result.getJobFlowId());
        // Add step to running cluster.
        HadoopJarStepConfig hadoopConfig2 = new HadoopJarStepConfig()
                .withJar("s3://bucket/YourJar.jar")
                .withMainClass("YourMainClass")
                .withArgs("s3://bucket/input", "s3://bucket/output");
        StepConfig customStep2 = new StepConfig("Step Name", hadoopConfig2);
        AddJobFlowStepsRequest request2 = new AddJobFlowStepsRequest()
                .withJobFlowId(jobFlowId)
                .withSteps(customStep3);
        AddJobFlowStepsResult result2 = client.addJobFlowSteps(request);


        // S3 download output
        downloadDirectory(bucketName, BUCKET_OUTPUT, OUTPUT, s3);
    }

    public static void downloadDirectory(String bucketName, String srcFolder, String tarFolder, AmazonS3Client s3) {
     	ObjectListing fileList = s3.listObjects(new ListObjectsRequest()
                        .withBucketName(bucketName)
                        .withPrefix(srcFolder + SUFFIX));
		for (S3ObjectSummary file : fileList.getObjectSummaries()) {
            downloadFile(bucketName, file.getKey(), tarFolder, s3);
		}
    }

    public static void downloadFile(String bucketName, String key, String storePath, AmazonS3Client s3) {
        // reference: http://stackoverflow.com/questions/6903255/aws-s3-java-sdk-download-file-help
        S3Object object;
        Path p = new Path(key);
        String fName = p.getName();
        object = s3.getObject(new GetObjectRequest(bucketName, key));
        InputStream reader = new BufferedInputStream(object.getObjectContent());
        File file = new File(storePath + SUFFIX + fName);
        try {
            OutputStream writer = new BufferedOutputStream(new FileOutputStream(file));
            int read = -1;
            while ( (read = reader.read()) != -1) {
                writer.write(read);
            }
            writer.flush();
            writer.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void uploadDirectory(String bucketName, String srcFolder, String tarFolder, AmazonS3Client s3) {
        File srcF = new File(srcFolder);
        File[] lof = srcF.listFiles();
        String key;
        for (File f : lof) {
            key = tarFolder + SUFFIX + f.getName();
            s3.putObject(bucketName, key, f);
        }
    }

    public static void createFolder(String bucketName, String folder, AmazonS3Client s3) {
        // create meta-data for your folder and set content-length to 0
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);
		// create empty content
		InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
		// create a PutObjectRequest passing the folder name suffixed by /
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
				folder + SUFFIX, emptyContent, metadata);
		// send request to S3 to create folder
		s3.putObject(putObjectRequest);
    }

    public static void deleteFolder(String bucketName, String folderName, AmazonS3 s3) {
		ObjectListing fileList = s3.listObjects(new ListObjectsRequest()
                        .withBucketName(bucketName)
                        .withPrefix(folderName + SUFFIX));
		for (S3ObjectSummary file : fileList.getObjectSummaries()) {
			s3.deleteObject(bucketName, file.getKey());
		}
		s3.deleteObject(bucketName, folderName);
	}
}
