package hidoop.mapreduce;

import java.io.IOException;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    public abstract class Context
            implements MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    }

    protected void setup(Context context
    ) throws IOException, InterruptedException {
        // NOTHING
    }

    @SuppressWarnings("unchecked")
    protected void map(KEYIN key, VALUEIN value,
                       Context context) throws IOException, InterruptedException {
        context.write((KEYOUT) key, (VALUEOUT) value);
    }

    protected void cleanup(Context context
    ) throws IOException, InterruptedException {
        // NOTHING
    }

    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        try {
            while (context.nextKeyValue()) {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
            }
        } finally {
            cleanup(context);
        }
    }
}
