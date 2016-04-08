package slave;

import hidoop.mapreduce.MapContext;
import hidoop.mapreduce.Mapper;

/**
 * Created by jon on 4/8/16.
 */
public class MapperRunner {
    MapContext context;
    Mapper mapper;
    public MapperRunner(String[] configs) {
        // TODO parse the configString


        // TODO get Mapper object

        // TODO prepare context
    }


    /**
     *
     * @return TRUE if the map succeeded, FALSE otherwise
     */
    public boolean run() {
        try {
            mapper.run((Mapper.Context)context);
            // TODO handle Mapper output in the context

        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }

        return true;
    }
}
