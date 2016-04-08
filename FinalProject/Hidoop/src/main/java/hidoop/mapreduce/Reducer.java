package hidoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by jon on 4/6/16.
 */
public class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    /**
     * The <code>Context</code> passed on to the {@link Reducer} implementations.
     */
    public abstract class Context
            implements ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    }

    /**
     * Called once at the start of the task.
     */
    protected void setup(Context context
    ) throws IOException, InterruptedException {
        // NOTHING
    }

    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     */
    @SuppressWarnings("unchecked")
    protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context
    ) throws IOException, InterruptedException {
        for (VALUEIN value : values) {
            context.write((KEYOUT) key, (VALUEOUT) value);
        }
    }

    /**
     * Called once at the end of the task.
     */
    protected void cleanup(Context context
    ) throws IOException, InterruptedException {
        // NOTHING
    }


    public void run(Context context) throws IOException, InterruptedException {
//        setup(context);
//        try {
//            while (context.nextKey()) {
//                reduce(context.getCurrentKey(), context.getValues(), context);
//                // If a back up store is used, reset it
//                Iterator<VALUEIN> iter = context.getValues().iterator();
//                if (iter instanceof ReduceContext.ValueIterator) {
//                    ((ReduceContext.ValueIterator<VALUEIN>) iter).resetBackupStore();
//                }
//            }
//        } finally {
//            cleanup(context);
//        }
        reduce(null, null, context);
    }
}
