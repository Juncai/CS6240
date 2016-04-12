package hidoop.mapreduce.counters;

import hidoop.mapreduce.Counter;

/**
 * Created by jon on 4/12/16.
 */
public class CounterGroup {

    public Counter findCounter(String type, String name) {
        return new Counter();
    }
}
