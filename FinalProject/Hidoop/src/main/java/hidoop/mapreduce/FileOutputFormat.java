package hidoop.mapreduce;

import hidoop.fs.Path;

import java.io.IOException;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class FileOutputFormat {
    public static void setOutputPath(Job job, Path outputDir) {
        job.getConfiguration().setOutputPath(outputDir.toString());
    }

}
