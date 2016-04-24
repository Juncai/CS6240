package hidoop.mapreduce.counters;

import hidoop.mapreduce.Counter;

// Author: Xinyuan Wang, Xi Wang
// Reference: github.com/apache/hadoop
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
