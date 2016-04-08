package hidoop;

/**
 * Created by jon on 4/8/16.
 */
public class Father {
    protected String words;

    public void setWords(String s) {
        this.words = s;
    }

    public void say() {
        System.out.println("Father says: " + words);
    }
}
