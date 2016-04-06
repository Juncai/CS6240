package sorting;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

// Author: Jun Cai
public class UtilsTest {

    @org.junit.Test
    public void testExtractBucketAndDir() throws Exception {
        String s3Path = "s3://cs6240sp16/climate";

        String[] res = Utils.extractBucketAndDir(s3Path);

        for (String s : res) {
            System.out.println(s);
        }

//        String g0 = "bucket";
//        String g1 = "dir";
//        String pt = "s3://(?<" + g0 + ">\\w+)/(?<" + g1 + ">\\w+)";
//        String tar = "s3://cs6240sp16/climate";
//        Pattern p = Pattern.compile(Consts.S3_URL_PATTERN);
//        Matcher m = p.matcher(s3Path);
//        if (m.find()) {
//            System.out.println("found");
//            System.out.println(m.group(Consts.BUCKET_GROUP));
//            System.out.println(m.group(Consts.DIR_GROUP));
//        }

    }
}