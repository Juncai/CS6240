package hidoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Author: Xinyuan Wang
// Reference: github.com/apache/hadoop
public class Text implements Writable<Text>{
    private String value;

    public Text(String value){
        this.value = value;
    }
    public Text(){}

    public void set(String v){this.value = v;}
    /**
     * serialize the object to the output stream, writes at most line of the data output
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException{
        out.writeUTF(this.value);
    }

    /**
     * deserialize the object from the input stream. The method will remove all the newline characters.
     * @param in
     * @throws IOException
     */
    public void readFrom(DataInput in) throws IOException{
        this.value = in.readUTF();
        if(this.value.contains("\n")) this.value = this.value.replace("\n", "");
    }

    /**
     * compare the value of two Strings
     * @param o
     * @return 1 if this value is greater than the objective's value, 0 if they are equel, -1 if this value is less
     * than the objective's value
     */
    public int compareTo(Text o){
        String str1 = this.value;
        String str2 = o.toString();
        int ret = str1.compareTo(str2);
        return ret > 0? 1: (ret == 0? 0: -1);
    }

    /**
     * check if this value has the same hashcode with the objective' value;
     * @param o
     * @return true if they have, else false
     */
    public boolean equals(Text o){
        return this.hashCode() == o.hashCode();
    }

    /**
     * find the value of this string
     * @return this value
     */
    public String toString(){return this.value;}

    /**
     * get the value's hashcode;
     * @return
     */
    @Override
    public int hashCode(){
        return this.value.hashCode();
    }
}
