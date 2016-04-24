package hidoop.util;

import org.junit.Test;

import static hidoop.util.Consts.BUCKET_GROUP;
import static hidoop.util.Consts.KEY_GROUP;
import static org.junit.Assert.*;

/**
 * Created by jon on 4/23/16.
 */
public class InputUtilsTest {
    @Test
    public void extractBucketAndDir() throws Exception {
        String path = "s3://juncai001/input_wc/something";
        String pattern = "s3://(?<" + BUCKET_GROUP + ">\\w+)/(?<" + KEY_GROUP + ">\\.+)";
        System.out.println(pattern);

//        String[] res = InputUtils.extractBucketAndDir(path, pattern);
        String[] res = path.split("/", 4);

        if (res != null) {
            for (String r : res) {
                System.out.println(r);
            }
        }

    }

}