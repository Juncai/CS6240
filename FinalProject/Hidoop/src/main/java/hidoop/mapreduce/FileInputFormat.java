package hidoop.mapreduce;

import hidoop.conf.Configuration;
import hidoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * Created by jon on 4/6/16.
 */
public class FileInputFormat {
    public static void addInputPath(Job job,
                                    Path path) throws IOException {
        Configuration conf = job.getConfiguration();
//        File f = new File(path.toString());
//        if (!f.exists()) throw new IOException("The input path doesn't exist");
        conf.setInputPath(path.toString());
    }

}
