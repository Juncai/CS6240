package hidoop.mapreduce;

import hidoop.mapreduce.counters.CounterGroup;

/**
 * Created by jon on 4/12/16.
 */
public class Counters {

    public CounterGroup getGroup(String groupName) {
        return new CounterGroup();
    }
}
