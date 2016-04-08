package hidoop.mapreduce;

import java.io.IOException;

/**
 * Created by jon on 4/6/16.
 */
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    public abstract class Context
            implements MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    }

    /**
     * Called once at the beginning of the task.
     */
    protected void setup(Context context
    ) throws IOException, InterruptedException {
        // NOTHING
    }

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     */
    @SuppressWarnings("unchecked")
    protected void map(KEYIN key, VALUEIN value,
                       Context context) throws IOException, InterruptedException {
        context.write((KEYOUT) key, (VALUEOUT) value);
    }

    /**
     * Called once at the end of the task.
     */
    protected void cleanup(Context context
    ) throws IOException, InterruptedException {
        // NOTHING
    }

    /**
     * Expert users can override this method for more complete control over the
     * execution of the Mapper.
     *
     * @param context
     * @throws IOException
     */
    public void run(Context context) throws IOException, InterruptedException {
//        setup(context);
//        try {
//            while (context.nextKeyValue()) {
//                map(context.getCurrentKey(), context.getCurrentValue(), context);
//            }
//        } finally {
//            cleanup(context);
//        }
        map(null, null, context);
    }
}
