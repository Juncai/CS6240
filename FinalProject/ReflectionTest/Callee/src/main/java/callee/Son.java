package callee;

import hidoop.Father;

/**
 * Created by jon on 4/8/16.
 */
public class Son extends Father {

    @Override
    public void say() {
        System.out.println("Son says: " + words);
    }
}
