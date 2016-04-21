package hidoop.fs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import hidoop.conf.Configuration;
import hidoop.util.Consts;
import hidoop.util.InputUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by jon on 4/12/16.
 */
public class FileSystem {
    private boolean isS3;
    private String inputPath;
    private String outputPath;
    private AmazonS3 s3;

    public FileSystem(String uri, Configuration conf) {
        isS3 = pathFromS3(uri.toString());
        inputPath = conf.inputPath;
        inputPath = conf.inputPath;

        // initialize s3 if necessary
        if (isS3) initS3();

    }

    private void initS3() {

        // load data from S3
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        s3 = new AmazonS3Client(credentials);
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
//        inputSummaryList = s3.listObjects(inputBucketInfo[0], inputBucketInfo[1]).getObjectSummaries();
    }

    public List<Path> getFileList(Path p) {
        List<Path> res = new ArrayList<Path>();
        if (isS3) {
            // TODO

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
        return p.toString().endsWith(Consts.TAR_GZ_EXT);
    }

    private static boolean pathFromS3(String path) {
        return path.startsWith(Consts.S3_URL_PREFIX);
    }

    public static FileSystem get(Configuration conf) {
        return get(conf.inputPath.toString(), conf);
    }

    public static FileSystem get(String uri, Configuration conf) {
        return new FileSystem(uri, conf);
    }

    public boolean exists(Path p) {
        File f = new File(p.toString());
        return f.exists();
    }

    public InputStream open(Path file) throws IOException {
        if (isS3) {
            return openS3File(file);
        } else {
            return openLocalFile(file);
        }
    }

    private InputStream openLocalFile(Path file) throws IOException {
        FileInputStream fis = new FileInputStream(file.toString());
        return isCompressed(file) ? new GZIPInputStream(fis) : fis;
    }

    private InputStream openS3File(Path file) throws IOException {
        // get s3 bucket info
        String[] inputBucketInfo = InputUtils.extractBucketAndDir(file.toString());
        S3Object object = s3.getObject(new GetObjectRequest(inputBucketInfo[0], inputBucketInfo[1]));
        GZIPInputStream gis = new GZIPInputStream(object.getObjectContent());
        return gis;
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
}

