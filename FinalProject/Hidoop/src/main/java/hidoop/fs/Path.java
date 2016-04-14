package hidoop.fs;

import hidoop.conf.Configuration;
import hidoop.util.Consts;

import java.io.IOException;
import java.util.List;

/**
 * Created by jon on 4/6/16.
 */
public class Path implements Comparable {
    private String pathStr;
    public String bucket;
    public List<String> keys;
    public Consts.FileSystemTypes type;

    public Path(String s) {
        pathStr = s;
    }

    public Path(String dir, String key) {
        this(dir + "/" + key);
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    //    public FileSystem getFileSystem(Configuration conf) throws IOException {
//        return FileSystem.get(this.toUri(), conf);
//    }
    public String toString() {
        return pathStr;
    }

    public static Path appendDirFile(Path dir, String fileName) {
        return new Path(dir.toString() + "/" + fileName);
    }

}
