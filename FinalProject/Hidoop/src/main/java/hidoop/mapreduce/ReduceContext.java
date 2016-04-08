package hidoop.mapreduce;

import java.io.IOException;

/**
 * Created by jon on 4/8/16.
 */
public interface ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    /**
     * Start processing next unique key.
     */
    public boolean nextKey() throws IOException, InterruptedException;

    /**
     * Iterate through the values for the current key, reusing the same value
     * object, which is stored in the context.
     *
     * @return the series of values associated with the current key. All of the
     * objects returned directly and indirectly from this method are reused.
     */
    public Iterable<VALUEIN> getValues() throws IOException, InterruptedException;

//    /**
//     * {@link Iterator} to iterate over values for a given group of records.
//     */
//    interface ValueIterator<VALUEIN> extends MarkableIteratorInterface<VALUEIN> {
//
//        /**
//         * This method is called when the reducer moves from one key to
//         * another.
//         *
//         * @throws IOException
//         */
//        void resetBackupStore() throws IOException;
//    }
}
