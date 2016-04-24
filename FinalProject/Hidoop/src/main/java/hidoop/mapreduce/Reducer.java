package hidoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    public abstract class Context
            implements ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    }

    protected void setup(Context context
    ) throws IOException, InterruptedException {
        // NOTHING
    }

    @SuppressWarnings("unchecked")
    protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context
    ) throws IOException, InterruptedException {
        for (VALUEIN value : values) {
            context.write((KEYOUT) key, (VALUEOUT) value);
        }
    }

    protected void cleanup(Context context
    ) throws IOException, InterruptedException {
        // NOTHING
    }


    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        try {
            while (context.nextKey()) {
                reduce(context.getCurrentKey(), context.getValues(), context);
            }
        } finally {
            cleanup(context);
        }
    }
}
