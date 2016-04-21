package hidoop.mapreduce;

/**
 * Created by jon on 4/12/16.
 */
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
