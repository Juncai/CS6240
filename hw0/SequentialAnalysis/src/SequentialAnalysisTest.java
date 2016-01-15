import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by jonca on 1/13/2016.
 */
public class SequentialAnalysisTest {

    @Test
    public void testSanityCheck() throws Exception {
        SequentialAnalysis sa = new SequentialAnalysis();
        String s = "1989,1,1,4,3,1989-01-04,\"AA\",19805,\"AA\",\"\",\"4\",12892,1289201,32575,\"LAX\",\"Los Angeles, CA\",\"CA\",\"06\",\"California\",91,12478,1247801,31703,\"JFK\",\"New York, NY\",\"NY\",\"36\",\"New York\",22,\"1200\",\"1236\",36.00,36.00,1.00,2,\"1200-1259\",,,,,\"2000\",\"2037\",37.00,37.00,1.00,2,\"2000-2059\",0,\"\",0,300,301,,1,2475,10,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,630.0";
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