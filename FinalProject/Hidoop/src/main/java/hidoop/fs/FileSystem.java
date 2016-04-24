package hidoop.fs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import hidoop.conf.Configuration;
import hidoop.util.Consts;
import hidoop.util.InputUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class FileSystem {
    private static FileSystem fs;
    private boolean isS3;
    private String inputPath;
    private String outputPath;
    private AmazonS3 s3;

    public FileSystem(Configuration conf) {
        inputPath = conf.inputPath;
        outputPath = conf.outputPath;
        isS3 = pathFromS3(inputPath) || pathFromS3(outputPath);

        // initialize s3 if necessary
        if (isS3) initS3();
    }

    public FileSystem(boolean isS3) {
        this.isS3 = isS3;
        if (this.isS3) {
            initS3();
        }
    }

    public static FileSystem get(Configuration conf) {
        if (fs == null) {
            fs = new FileSystem(conf);
        }
        return fs;
    }

    public static FileSystem get(boolean isS3) {
        if (fs == null) {
            fs = new FileSystem(isS3);
        }
        return fs;
    }

    private void initS3() {
        // load data from S3
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        s3 = new AmazonS3Client(credentials);
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
    }

    public List<Path> getFileList(Path p) {
        List<Path> res = new ArrayList<Path>();
        if (pathFromS3(p.toString())) {
            String[] inputBucketInfo = InputUtils.extractBucketAndDir(p.toString());
            List<S3ObjectSummary> summaryList = s3.listObjects(inputBucketInfo[0], inputBucketInfo[1]).getObjectSummaries();
            for (int i = 1; i < summaryList.size(); i++) {
                res.add(new Path(summaryList.get(i).getBucketName(), summaryList.get(i).getKey(), true));
            }
        } else {
            File path = new File(p.toString());
            if (path.isFile()) {
                res.add(new Path(p.toString()));
            }
            if (path.isDirectory()) {
                for (File f : path.listFiles()) {
                    res.add(new Path(f.getAbsolutePath()));
                }
            }
        }

        return res;
    }

    private boolean isCompressed(Path p) {
        return p.toString().endsWith(Consts.CSV_GZ_EXT);
    }

    private static boolean pathFromS3(String path) {
        return path.startsWith(Consts.S3_URL_PREFIX);
    }


    public boolean exists(Path p) {
        if (pathFromS3(p.toString())) {
            String[] inputBucketInfo = InputUtils.extractBucketAndDir(p.toString());
            return s3.doesObjectExist(inputBucketInfo[0], inputBucketInfo[1]);
        } else {
            File f = new File(p.toString());
            return f.exists();
        }
    }

    public InputStream open(Path file) throws IOException {
        if (pathFromS3(file.toString())) {
            return openS3File(file);
        } else {
            return openLocalFile(file);
        }
    }

    private InputStream openLocalFile(Path file) throws IOException {
        FileInputStream fis = new FileInputStream(file.toString());
        boolean isComp = isCompressed(file);
        return isComp ? new GZIPInputStream(fis) : fis;
    }

    private InputStream openS3File(Path file) throws IOException {
        // get s3 bucket info
        String[] inputBucketInfo = InputUtils.extractBucketAndDir(file.toString());
        S3Object object = s3.getObject(new GetObjectRequest(inputBucketInfo[0], inputBucketInfo[1]));
        if (isCompressed(file)) {
            return new GZIPInputStream(object.getObjectContent());
        } else {
            return object.getObjectContent();
        }
    }

    public void createOutputFile(Path filePath, Iterable<String> content, boolean isInter) throws IOException {
        if (isInter) {
            File f = new File(filePath.toString());
            f.createNewFile();
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)));
            for (String line : content) {
                bw.write(line + Consts.END_OF_LINE);
            }
            bw.flush();
            bw.close();
        } else if (isS3) {
            // TODO create a local tmp file
        }
    }

    public OutputStream create(Path p) throws IOException {
        File f = new File(p.toString());
        f.createNewFile();
        return new FileOutputStream(p.toString());
    }

    public static void createDir(Path dir) throws IOException {
        File newDir = new File(dir.toString());
        if (!newDir.exists()) {
            newDir.mkdir();
        } else {
            System.out.println(newDir.toString());
            throw new IOException("Output dir exists!");
        }
    }

    public static void createDirIfNotExisted(Path dir) throws IOException {
        File newDir = new File(dir.toString());
        if (!newDir.exists()) {
            newDir.mkdir();
        }
    }

    public static void removeFile(Path p) {
        File f = new File(p.toString());
        f.deleteOnExit();
    }

    public static String getFileName(Path p) {
        String[] parts = p.toString().split("/");
        return parts[parts.length - 1];
    }

    public void save(Path file) {
        if (isS3) {
            // TODO find the local tmp file and send to s3
        }
    }

    public void uploadToS3(Path input, Path output) {
        String[] outputBucketInfo = InputUtils.extractBucketAndDir(output.toString());
        String bucketName = outputBucketInfo[0];
        String key = outputBucketInfo[1];
        File f = new File(input.toString());
        s3.putObject(new PutObjectRequest(bucketName, key, f));
    }
}

