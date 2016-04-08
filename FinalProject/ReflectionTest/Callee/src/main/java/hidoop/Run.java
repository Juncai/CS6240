package hidoop;

/**
 * Created by jon on 4/8/16.
 */
public class Run {
    private String className;
    private Class c;
    public Run(String className) {
        this.className = className;
    }

    public Run(Class c) {
        this.c = c;
    }

    public void run() {
        if (c != null) {
            try {
                Father f = (Father)c.newInstance();
                f.setWords("haha");
                f.say();
            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println("something wrong!");
            }
        }
    }
}
