package hidoop.mapreduce;

import java.util.List;
import java.util.Map;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public interface MapContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
  extends TaskInputOutputContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

  /**
   * Get the input split for this map.
   */
    public long getCounterValue();
    public Map<Integer, List<String>> getOutputBuffer();

}
