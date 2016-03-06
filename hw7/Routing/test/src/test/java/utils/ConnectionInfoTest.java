package utils;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by jon on 2/14/16.
 */
public class ConnectionInfoTest {

    @Test
    public void testProcessTS() throws Exception {
        List<Date> lod = new ArrayList<Date>();
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
        Date d1 = sf.parse("2015-11-15");
        Date d2 = sf.parse("2016-11-15");
        lod.add(d2);
        lod.add(d1);
        Collections.sort(lod);
        assertEquals(lod.get(0).getTime(), d1.getTime());
    }
}