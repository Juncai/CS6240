package hidoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * written by xinyuan
 */
public class DoubleWritable implements Writable<DoubleWritable> {
    private double value;

    public DoubleWritable(){};
    public DoubleWritable(double value){this.value = value;}

    public void set(double value){this.value = value;}
    public double get(){return this.value;}

    /**
     * serialize the object to the output stream
     * @param DataOutputStream out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException{
        out.writeDouble(this.value);
    }

    /**
     * deserialize the object to the input stream
     * @param DataInputStream in
     * @throws IOException
     */
    public void readFrom(DataInput in) throws IOException{
        this.value = in.readDouble();
    }

    /**
     * compare this value to the objective return 1 if greater -1 if less 0 if equal
     * @param DoubleWritable o
     * @return int
     */
    public int compareTo(DoubleWritable o){
        double v1 = this.value;
        double v2 = o.get();
        return v1 > v2? 1: (v1 == v2? 0: -1);
    }
    /**
     * convert double to string;
     * @return double
     */
    public String toString(){
        return Double.toString(this.value);
    }
}
