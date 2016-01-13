import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by jonca on 1/13/2016.
 */
public class SequentialAnalysisTest {

    @Test
    public void testSanityCheck() throws Exception {
        SequentialAnalysis sa = new SequentialAnalysis();
        String s = "2013,1,1,17,4,2013-01-17,\"9E\",20363,\"9E\",\"N923XJ\",\"3324\",11298,1129802,30194,\"DFW\",\"Dallas/Fort Worth, TX\",\"TX\",\"48\",\"Texas\",74,12478,1247801,31703,\"JFK\",\"New York, NY\",\"NY\",\"36\",\"New York\",22,\"1045\",\"1038\",-7.00,0.00,0.00,-1,\"1000-1059\",10.00,\"1048\",\"1443\",8.00,\"1505\",\"1451\",-14.00,0.00,0.00,-1,\"1500-1559\",0,\"\",0,200,193,175,1,1391,6,,,,,,,,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,668.0";
//        String s = "2013,1,1,22,2,2013-01-22,\"9E\",20363,\"9E\",\"N601LR\",\"3324\",11298,1129802,30194,\"DFW\",\"Dallas/Fort Worth, TX\",\"TX\",\"48\",\"Texas\",74,12478,1247801,31703,\"JFK\",\"New York, NY\",\"NY\",\"36\",\"New York\",22,\"1045\",\"1054\",9.00,9.00,0.00,0,\"1000-1059\",9.00,\"1103\",\"1454\",8.00,\"1505\",\"1502\",-3.00,0.00,0.00,-1,\"1500-1559\",0,\"\",0,200,188,171,1,1391,6,,,,,,,,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,556.0";
        String[] res = sa.parseCSVLine(s);
        boolean valid = sa.sanityCheck(res);
        assertTrue(valid);
    }

    @Test
    public void testParseCSVLine() throws Exception {
        SequentialAnalysis sa = new SequentialAnalysis();
        String s = "2013,1,1,22,2,2013-01-22,\"9E\",20363,\"9E\",\"N601LR\",\"3324\",11298,1129802,30194,\"DFW\",\"Dallas/Fort Worth, TX\",\"TX\",\"48\",\"Texas\",74,12478,1247801,31703,\"JFK\",\"New York, NY\",\"NY\",\"36\",\"New York\",22,\"1045\",\"1054\",9.00,9.00,0.00,0,\"1000-1059\",9.00,\"1103\",\"1454\",8.00,\"1505\",\"1502\",-3.00,0.00,0.00,-1,\"1500-1559\",0,\"\",0,200,188,171,1,1391,6,,,,,,,,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,556.0";
        String[] res = sa.parseCSVLine(s);
//        for (String ss : res) {
//            System.out.println(ss);
//        }
        for (int i = 0; i < res.length; i++) {
            String ss = res[i];
            System.out.println(i + " " + ss);
        }
        assertEquals(res.length, 110);
    }
}