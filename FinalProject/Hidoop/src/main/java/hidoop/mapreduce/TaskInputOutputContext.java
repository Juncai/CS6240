package hidoop.mapreduce;

import java.io.IOException;

/**
 * Created by jon on 4/7/16.
 */
public interface TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    /**
     * Advance to the next key, value pair, returning null if at end.
     *
     * @return the key object that was read into, or null if no more
     */
    public boolean nextKeyValue() throws IOException, InterruptedException;

    /**
     * Get the current key.
     *
     * @return the current key object or null if there isn't one
     * @throws IOException
     * @throws InterruptedException
     */
    public KEYIN getCurrentKey() throws IOException, InterruptedException;

    /**
     * Get the current value.
     *
     * @return the value object that was read into
     * @throws IOException
     * @throws InterruptedException
     */
    public VALUEIN getCurrentValue() throws IOException, InterruptedException;

    /**
     * Generate an output key/value pair.
     */
    public void write(KEYOUT key, VALUEOUT value)
            throws IOException, InterruptedException;
}
