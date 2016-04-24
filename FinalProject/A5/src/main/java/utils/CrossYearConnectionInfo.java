package utils;

import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jon on 2/18/16.
 */
public class CrossYearConnectionInfo extends ConnectionInfo {
    public CrossYearConnectionInfo() {
        depTSs = new ArrayList<LocalDateTime[]>();
        arrTSs = new ArrayList<LocalDateTime[]>();
    }

    public void updateDeps(List<LocalDateTime[]> deps) {
        depTSs.addAll(deps);
    }

    public void updateArrs(List<LocalDateTime[]> arrs) {
        arrTSs.addAll(arrs);
    }

    @Override
    public int countMissedConnections() {
        return missedConBetweenLOD(depTSs, arrTSs);
    }
}
