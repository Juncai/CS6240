package sorting;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Author: Jun Cai
public class Utils {
    public static String[] extractBucketAndDir(String s3Path) {
        Pattern p = Pattern.compile(Consts.S3_URL_PATTERN);
        Matcher m = p.matcher(s3Path);
        String[] res = new String[2];
        if (m.find()) {
            res[0] = m.group(Consts.BUCKET_GROUP);
            res[1] = m.group(Consts.DIR_GROUP);
        }
        return res;
    }
}
