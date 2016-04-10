package hidoop.fs;

import hidoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by jon on 4/6/16.
 */
public class Path implements Comparable {
    private String pathStr;
    public Path(String s) {
        pathStr = s;
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

}
