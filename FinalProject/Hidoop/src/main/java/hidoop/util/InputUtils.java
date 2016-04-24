package hidoop.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class InputUtils {
    public static String[] extractBucketAndDir(String s3Path) {
        String[] tmp = s3Path.split("/", 4);
        String[] res = new String[]{tmp[2], tmp[3]};

        return res;
    }

    public static String[] extractBucketAndDir_bak(String s3Path) {
        Pattern p = Pattern.compile(Consts.S3_URL_PATTERN);
        Matcher m = p.matcher(s3Path);
        if (m.find()) {
            String[] res = new String[2];
            res[0] = m.group(Consts.BUCKET_GROUP);
            res[1] = m.group(Consts.KEY_GROUP);
            return res;
        }
        return null;
    }

    public static String[] extractBucketAndDir(String s3Path, String pattern) {
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(s3Path);
        if (m.find()) {
            String[] res = new String[2];
            res[0] = m.group(Consts.BUCKET_GROUP);
            res[1] = m.group(Consts.KEY_GROUP);
            return res;
        }
        return null;
    }

    public static String[] extractKey(String s) {
        return s.split(Consts.KEY_VALUE_DELI, 2);
    }
}
