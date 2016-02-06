package analysis;

import org.apache.commons.cli.*;

public class OTPAnalysis {
    final static String INPUT = "input";
    final static String PARALLEL = "p";
    final static String MEAN = "mean";
    final static String MEDIAN = "median";
    final static String TYPE = "t";
    final static String FASTMEDIAN = "fast";

    public static void main(String[] args) throws InterruptedException, ParseException {
        String dir;
        String typeArg;
        boolean isMean;
        boolean fastMedian = false;

        // argument parsing
        Options options = new Options();

        options.addOption(PARALLEL, false, "run in parallel mode");
        options.addOption(FASTMEDIAN, false, "using fast median calculation.");

        Option input = new Option(INPUT, "data path");
        input.setRequired(true);
        input.setArgs(1);
        options.addOption(input);

        Option type = new Option(TYPE, "the type of analysis");
        type.setRequired(true);
        type.setArgs(1);
        options.addOption(type);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        dir = cmd.getOptionValue(INPUT);

        typeArg = cmd.getOptionValue(TYPE);
        if (typeArg.equals(MEAN)) {
            isMean = true;
        } else if (typeArg.equals(MEDIAN)) {
            isMean = false;
        } else {
            System.out.println("Invalid analysis type!");
            return;
        }

        if (cmd.hasOption(FASTMEDIAN)) fastMedian = true;

        long start = System.currentTimeMillis();
        if (cmd.hasOption(PARALLEL)) {
            // run in parallel mode
            ThreadedAnalyzer ta = new ThreadedAnalyzer(isMean, fastMedian);
            ta.analyze(dir);
            ta.printResults();
        } else {
            // run in sequential mode
            SequentialAnalyzer sa = new SequentialAnalyzer(isMean, fastMedian);
            sa.batchAnalyze(dir);
            sa.printResults();
        }
        System.out.println((System.currentTimeMillis() - start) / 1000);
    }
}