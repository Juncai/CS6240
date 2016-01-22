import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by jonca on 1/13/2016.
 */
public class SequentialAnalysisTest {

    @Test
    public void testSanityCheck() throws Exception {
        SequentialAnalysis sa = new SequentialAnalysis();
        String s = "1989,1,1,8,7,1989-01-08,\"AA\",19805,\"AA\",\"\",\"45\",10693,1069301,30693,\"BNA\",\"Nashville, TN\",\"TN\",\"47\",\"Tennessee\",54,13303,1330302,32467,\"MIA\",\"Miami, FL\",\"FL\",\"12\",\"Florida\",33,\"2025\",\"2234\",129.00,129.00,1.00,8,\"2000-2059\",,,,,\"2327\",\"0128\",121.00,121.00,1.00,8,\"2300-2359\",0,\"\",0,122,114,,1,806,4,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,320.86";
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