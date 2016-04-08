package hidoop.mapreduce;

import hidoop.fs.Path;

import java.io.IOException;

/**
 * Created by jon on 4/6/16.
 */
public class FileOutputFormat {
    public static void setOutputPath(Job job, Path outputDir) {
//        try {
//            outputDir = outputDir.getFileSystem(job.getConfiguration()).makeQualified(
//                    outputDir);
//        } catch (IOException e) {
//            // Throw the IOException as a RuntimeException to be compatible with MR1
//            throw new RuntimeException(e);
//        }
//        job.getConfiguration().set(FileOutputFormat.OUTDIR, outputDir.toString());
        job.getConfiguration().setOutputPath(outputDir);
    }

}
