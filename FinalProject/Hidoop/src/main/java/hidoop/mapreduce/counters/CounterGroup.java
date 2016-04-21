package hidoop.mapreduce.counters;

import hidoop.mapreduce.Counter;

/**
 * Created by jon on 4/12/16.
 */
public class CounterGroup {
    private Counter c;
    public CounterGroup(){};
    public Counter findCounter(String type, String name) {
        return this.c;
    }
    public void setCounter(Counter c){
        this.c = c;
    }
}
