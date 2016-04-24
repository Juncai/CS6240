package hidoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Author: Xinyuan Wang
// Reference: github.com/apache/hadoop
public class LongWritable implements Writable<LongWritable> {
    private long value;

    public LongWritable(){}
    public LongWritable(long value){this.value = value;}
    public LongWritable(String valueStr){
        this.value = Long.parseLong(valueStr);
    }

    public void set(long value){this.value = value;}
    public long get(){return this.value;}

    /**
     * serialize this.value to output stream
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException{
        out.writeLong(this.value);
    }

    /**
     * deserialize this value from input stream
     * @param in
     * @throws IOException
     */
    @Override
    public void readFrom(DataInput in) throws IOException{
        this.value = in.readLong();
    }

    /**
     * compare this value to the objective
     * @param o
     * @return return 1 if greater -1 if less 0 if equal
     */
    @Override
    public int compareTo(LongWritable o){
        long v1 = this.value;
        long v2 = o.get();
        return v1 > v2? 1: (v1 == v2? 0: -1);
    }

    /**
     * convert double to string;
     * @return double
     */
    public String toString(){
        return Long.toString(this.value);
    }
    public int hashCode(){
        return Long.valueOf(this.value).hashCode();
    }
}
