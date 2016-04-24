package hidoop.fs;

import hidoop.util.Consts;

import java.util.List;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class Path implements Comparable {
    private String pathStr;
    public String bucket;
    public String key;
    public List<String> keys;
    public Consts.FileSystemTypes type;

    public Path(String s) {
        pathStr = s;
    }

    public Path(String dir, String key) {
        this(dir + "/" + key);
        type = Consts.FileSystemTypes.LOCAL;
    }

    public Path(String dir, String key, boolean isS3) {
        this(dir + "/" + key);
        if (isS3) {
            bucket = dir;
            this.key = key;
            type = Consts.FileSystemTypes.S3;
            pathStr = Consts.S3_URL_PREFIX + pathStr;
        } else {
            type = Consts.FileSystemTypes.LOCAL;
        }
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    public String toString() {
        return pathStr;
    }

    public static Path appendDirFile(Path dir, String fileName) {
        return new Path(dir.toString() + "/" + fileName);
    }

}
