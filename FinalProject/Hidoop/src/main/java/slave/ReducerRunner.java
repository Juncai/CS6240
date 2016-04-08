package slave;

import hidoop.mapreduce.ReduceContext;
import hidoop.mapreduce.Reducer;

/**
 * Created by jon on 4/8/16.
 */
public class ReducerRunner {
    ReduceContext context;
    Reducer reducer;

    public ReducerRunner(String[] configs) {
        // TODO parse the configString


        // TODO get Mapper object

        // TODO prepare context
    }


    /**
     * @return TRUE if the map succeeded, FALSE otherwise
     */
    public boolean run() {
        try {
            reducer.run((Reducer.Context) context);
            // TODO handle Mapper output in the context

        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }

        return true;
    }
}
