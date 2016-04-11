package hidoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * written by xinyuan
 */
public interface Writable<T> extends Comparable<T> {
    /**
     * serialize the object to the output stream
     * @param out
     * @throws IOException
     */
    void write(DataOutput out) throws IOException;

    /**
     * deserialize the object to the input stream
     * @param in
     * @throws IOException
     */
    void readFrom(DataInput in) throws IOException;
}
