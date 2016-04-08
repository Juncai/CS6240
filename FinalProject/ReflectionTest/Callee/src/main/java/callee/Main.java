package callee;

import hidoop.Run;

/**
 * Created by jon on 4/8/16.
 */
public class Main {
    public static void main(String[] args) {
        Run r = new Run(Son.class);
        r.run();
    }
}
