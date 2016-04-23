package hidoop.mapreduce;

import hidoop.fs.Path;

import java.util.List;
import java.util.Map;

/**
 * Created by jon on 4/7/16.
 */
public interface MapContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
  extends TaskInputOutputContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

  /**
   * Get the input split for this map.
   */
    public long getCounterValue();
    public Map<Integer, List<String>> getOutputBuffer();

}
