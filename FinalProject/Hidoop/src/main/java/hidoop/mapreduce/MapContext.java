package hidoop.mapreduce;

/**
 * Created by jon on 4/7/16.
 */
public interface MapContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
  extends TaskInputOutputContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

  /**
   * Get the input split for this map.
   */
//  public InputSplit getInputSplit();

}
