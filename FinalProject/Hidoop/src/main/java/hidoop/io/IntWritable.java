package hidoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jon on 4/6/16.
 */
public class IntWritable implements Writable<IntWritable>{
    private int value;
    public IntWritable(){}
    public IntWritable(int value){this.value = value;}
    public IntWritable(String valueStr){
        this.value = Integer.parseInt(valueStr);
    }

    public void set(int value){this.value = value;}

    public int get(){return this.value;}
    /**
     * serialize this.value to output stream
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.value);
    }

    /**
     * deserialize this value from input stream
     * @param in
     * @throws IOException
     */
    @Override
    public void readFrom(DataInput in) throws IOException{
        this.value = in.readInt();
    }

    /**
     * compare this value to the objective return 1 if greater -1 if less 0 if equal
     * @param o
     * @return
     */
    @Override
    public int compareTo(IntWritable o){
        int v1 = this.value;
        int v2 = o.get();
        return v1 > v2? 1: (v1 == v2? 0: -1);
    }

    public String toString(){
        return Integer.toString(this.value);
    }
    public int hashCode(){
        return Integer.valueOf(this.value).hashCode();
    }
}
