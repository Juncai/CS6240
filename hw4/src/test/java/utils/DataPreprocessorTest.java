package utils;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by jon on 2/9/16.
 */
public class DataPreprocessorTest {

    @org.junit.Test
    public void testProcessLine() throws Exception {

    }

    @org.junit.Test
    public void testResetMatrices() throws Exception {

    }

    @org.junit.Test
    public void testReplaceMatrices() throws Exception {

    }

    @org.junit.Test
    public void testUpdateMatrices() throws Exception {

    }

    @org.junit.Test
    public void testGetNewLOM() throws Exception {
        List<Double[][]> act = DataPreprocessor.getNewLOM();
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 1; j++) {
                assertEquals(.0, act.get(0)[i][j], 0.1);
                assertEquals(.0, act.get(1)[i][j], 0.1);
                act.get(0)[i][j] = 1.0;
                act.get(1)[i][j] = 1.0;
            }
        }
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 1; j++) {
                assertEquals(1.0, act.get(0)[i][j], 0.1);
                assertEquals(1.0, act.get(1)[i][j], 0.1);
            }
        }
    }

    @org.junit.Test
    public void testGetEmptyMatrix() throws Exception {

    }

    @org.junit.Test
    public void testGetYear() throws Exception {

    }

    @org.junit.Test
    public void testGetMonth() throws Exception {

    }

    @org.junit.Test
    public void testGetPrice() throws Exception {

    }

    @org.junit.Test
    public void testGetXTY() throws Exception {

    }

    @org.junit.Test
    public void testMatrixMultiply() throws Exception {

    }

    @org.junit.Test
    public void testTranspose() throws Exception {
        double[][] x = {{2.3, 4.5}};
        double[][] xe = {{2.3}, {4.5}};
        double[][] xa = DataPreprocessor.transpose(x);
        assertArrayEquals(xe, xa);

    }

    @org.junit.Test
    public void testSerializeMatrices() throws Exception {

    }

    @org.junit.Test
    public void testSerializeMatrix() throws Exception {

    }

    @org.junit.Test
    public void testDeserializeMatrices() throws Exception {

    }

    @org.junit.Test
    public void testIsActiveCarrier() throws Exception {

    }

    @org.junit.Test
    public void testFit() throws Exception {

    }

    @org.junit.Test
    public void testInverse() throws Exception {

    }
}