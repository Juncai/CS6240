package analysis;


public class OTPAnalysis {

    public static void main(String[] args) throws InterruptedException {
        String inputStr = "-input=";
        String pStr = "-p";

        // naive parser
        if (args.length < 1 || args.length > 2
                || (args.length == 1 && !args[0].startsWith(inputStr))
                || (args.length == 2 && (!args[0].equals(pStr) || !args[1].startsWith(inputStr)))) {
            System.out.println("The arguments should be: [-p] -input=DIR");
            return;
        }

        // single-threaded
        if (args.length == 1) {
            System.out.println("Single-threaded analysis begins!");
            String dir = args[0].substring(7);
            SequentialAnalyzer sa = new SequentialAnalyzer();
            sa.batchAnalyze(dir);
            sa.printResults();
        }

        // multi-threaded
        if (args.length == 2) {
            System.out.println("Multi-threaded analysis begins!");
            String dir = args[1].substring(7);
            ThreadedAnalyzer ta = new ThreadedAnalyzer();
            ta.analyze(dir);
            ta.printResults();
        }
    }
}