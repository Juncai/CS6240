package hidoop.fs;

import hidoop.conf.Configuration;
import hidoop.conf.Configured;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

/**
 * Created by jon on 4/7/16.
 * Need to support two file systems:
 * 1. local FS
 * 2. S3
 */
public class FileSystem extends Configured {
//    public static FileSystem get(final URI uri, final Configuration conf,
//                                 final String user) throws IOException, InterruptedException {
//        String ticketCachePath =
//                conf.get(CommonConfigurationKeys.KERBEROS_TICKET_CACHE_PATH);
//        UserGroupInformation ugi =
//                UserGroupInformation.getBestUGI(ticketCachePath, user);
//        return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
//            @Override
//            public FileSystem run() throws IOException {
//                return get(uri, conf);
//            }
//        });
//    }

}
