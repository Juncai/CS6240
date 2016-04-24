package hidoop.mapreduce;

import hidoop.conf.Configuration;
import hidoop.fs.Path;

import java.io.File;
import java.io.IOException;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class FileInputFormat {
    public static void addInputPath(Job job,
                                    Path path) throws IOException {
        Configuration conf = job.getConfiguration();
        conf.setInputPath(path.toString());
    }

}
