package hidoop.mapreduce;

import hidoop.conf.Configuration;
import hidoop.fs.Path;

import java.io.IOException;

/**
 * Created by jon on 4/6/16.
 */
public class FileInputFormat {
    public static void addInputPath(Job job,
                                    Path path) throws IOException {
        Configuration conf = job.getConfiguration();
//        path = path.getFileSystem(conf).makeQualified(path);
//        String dirStr = StringUtils.escapeString(path.toString());
//        String dirs = conf.get(INPUT_DIR);
//        conf.set(INPUT_DIR, dirs == null ? dirStr : dirs + "," + dirStr);
        conf.setInputPath(path);
    }

}
