package hidoop.mapreduce;

// Author: Xinyuan Wang, Xi Wang
// Reference: github.com/apache/hadoop
public class Counter {
    private long value;

    public Counter() {
        value = 0;
    }
    public Counter(long value){
       this.value = value;
    }

    public void increment(){
        this.value += 1;
    }
    public Counter join(Counter c){
        this.value += c.getValue();
        return this;
    }
    public long getValue() {
        return value;
    }
}
