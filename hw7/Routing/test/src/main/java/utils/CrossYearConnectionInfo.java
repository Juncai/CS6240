package utils;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jon on 2/18/16.
 */
public class CrossYearConnectionInfo extends ConnectionInfo {
    public CrossYearConnectionInfo() {
        depTSs = new ArrayList<DateTime[]>();
        arrTSs = new ArrayList<DateTime[]>();
    }

    public void updateDeps(List<DateTime[]> deps) {
        depTSs.addAll(deps);
    }

    public void updateArrs(List<DateTime[]> arrs) {
        arrTSs.addAll(arrs);
    }

    @Override
    public int countMissedConnections() {
        return missedConBetweenLOD(depTSs, arrTSs);
    }
}
