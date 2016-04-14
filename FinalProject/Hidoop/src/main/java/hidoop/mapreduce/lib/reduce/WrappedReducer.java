package hidoop.mapreduce.lib.reduce;

import hidoop.mapreduce.ReduceContext;
import hidoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jon on 4/13/16.
 */
public class WrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    public Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context
    getReducerContext(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext) {
        return new Context(reduceContext);
    }

    public class Context
            extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

        protected ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext;

        public Context(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext) {
            this.reduceContext = reduceContext;
        }

        @Override
        public KEYIN getCurrentKey() throws IOException, InterruptedException {
            return reduceContext.getCurrentKey();
        }

        @Override
        public VALUEIN getCurrentValue() throws IOException, InterruptedException {
            return reduceContext.getCurrentValue();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return reduceContext.nextKeyValue();
        }

        @Override
        public void write(KEYOUT key, VALUEOUT value) throws IOException,
                InterruptedException {
            reduceContext.write(key, value);
        }

        @Override
        public Iterable<VALUEIN> getValues() throws IOException,
                InterruptedException {
            return reduceContext.getValues();
        }

        @Override
        public boolean nextKey() throws IOException, InterruptedException {
            return reduceContext.nextKey();
        }

        @Override
        public void close() {
            reduceContext.close();
        }

    }
}
