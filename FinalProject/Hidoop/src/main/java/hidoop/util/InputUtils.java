package hidoop.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jon on 4/9/16.
 */
public class InputUtils {
    public static String[] extractBucketAndDir(String s3Path) {
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
}
