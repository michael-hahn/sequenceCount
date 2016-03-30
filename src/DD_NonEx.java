//import org.apache.spark.api.java.JavaRDD;
//
//import java.util.Calendar;
//import java.util.List;
//import java.util.logging.FileHandler;
//import java.util.logging.Level;
//import java.util.logging.LogManager;
//import java.util.logging.Logger;
//
///**
// * Created by Michael on 10/13/15.
// */
//
//public class DD_NonEx<T> {
//    public JavaRDD<T>[] split(JavaRDD<T> inputRDD, int numberOfPartitions, userSplit<T> splitFunc) {
//        return splitFunc.usrSplit(inputRDD, numberOfPartitions);
//    }
//
//    //test returns true if the test fails
//    public boolean test(JavaRDD<T> inputRDD, userTest<T> testFunc, LogManager lm, FileHandler fh) {
//        return testFunc.usrTest(inputRDD, lm, fh);
//    }
//
//    private void dd_helper(JavaRDD<T> inputRDD, int numberOfPartitions, userTest<T> testFunc, userSplit<T> splitFunc,
//                           LogManager lm, FileHandler fh) {
//
//        Logger logger = Logger.getLogger(DD_NonEx.class.getName());
//        logger.addHandler(fh);
//
//        JavaRDD<T> rdd = inputRDD;
//        int partitions = numberOfPartitions;
//        int runTime = 1;
//        int bar_offset = 0;
//
//        while (true) {
//
//            java.sql.Timestamp startTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
//            long startTime = System.nanoTime();
//
//            long sizeRDD = rdd.count();
//
//            boolean assertResult = test(rdd, testFunc, lm, fh);
//            //assert(assertResult == true);
//            //System.out.println("Assertion passed ready to split");
//            if (!assertResult) {
//                long endTime = System.nanoTime();
//                logger.log(Level.INFO, "The #" + runTime + " run is done");
//                logger.log(Level.INFO, "This run takes " + (endTime - startTime)/1000 + " microseconds");
//                logger.log(Level.INFO, "This data size is " + sizeRDD);
//                return;
//            }
//
//            if (rdd.count() <= 1) {
//                //Cannot further split RDD
//                long endTime = System.nanoTime();
//                logger.log(Level.INFO, "The #" + runTime + " run is done");
//                logger.log(Level.INFO, "RDD Only Holds One Line - End of This Branch of Search");
//                logger.log(Level.WARNING, "Delta Debugged Error inducing inputs: " + rdd.collect());
//                logger.log(Level.INFO, "This run takes " + (endTime - startTime)/1000 + " microseconds");
//                return;
//            }
//
//            /*****************
//             * **********
//             */
//            //rdd.cache();
//
//            //logger.log(Level.INFO, "Start Splitting...");
//            JavaRDD<T>[] rddList = split(rdd, partitions, splitFunc);
//            //logger.log(Level.INFO, "Split to " + partitions + " partitions is done.");
//
//
//            boolean rdd_failed = false;
//            boolean rddBar_failed = false;
//            JavaRDD<T> next_rdd = rdd;
//            int next_partitions = partitions;
//
////            for (int i = 0; i < partitions; i++) {
////                logger.log(Level.INFO, "Generating subRDD id:" + rddList[i].id() + " with line counts: " + rddList[i].count());
////            }
//
//            for (int i = 0; i < partitions; i++) {
//                //System.out.println("Testing subRDD id:" + rddList[i].id());
//                boolean result = test(rddList[i], testFunc, lm, fh);
//                //System.out.println("Testing is done");
//                if (result) {
//                    rdd_failed = true;
//                    next_rdd = rddList[i];
//                    next_partitions = 2;
//                    bar_offset = 0;
//                    break;
//                }
//            }
//
//            //check complements
//            if (!rdd_failed) {
//                for (int j = 0; j < partitions; j++) {
//                    int i = (j + bar_offset) % partitions;
//                    JavaRDD<T> rddBar = rdd.subtract(rddList[i]);
//                    boolean result = test(rddBar, testFunc, lm, fh);
//                    if (result) {
//                        rddBar_failed = true;
//                        next_rdd = next_rdd.intersection(rddBar);
//                        next_partitions = next_partitions - 1;
//
//                        bar_offset = i;
//                        break;
//                    }
//                }
//            }
//
//            if (!rdd_failed && !rddBar_failed) {
//                long rddSiz = rdd.count();
//                if (rddSiz <= 2) {
//                    //Cannot further split RDD
//                    long endTime = System.nanoTime();
//                    logger.log(Level.INFO, "The #" + runTime + " run is done");
//                    logger.log(Level.INFO, "End of This Branch of Search");
//                    logger.log(Level.INFO, "This data size is " + sizeRDD);
//                    logger.log(Level.WARNING, "Delta Debugged Error inducing inputs: " + rdd.collect());
//                    logger.log(Level.INFO, "This run takes " + (endTime - startTime)/1000 + " microseconds");
//                    return;
//                }
//
//                next_partitions = Math.min((int) rdd.count(), partitions * 2);
//                //logger.log(Level.INFO, "DD: Increase granularity to: " + next_partitions);
//            }
//            long endTime = System.nanoTime();
//            logger.log(Level.INFO, "Finish the " + runTime + "th run of Non-exhaustive DD, taking " + (endTime - startTime) / 1000 + " microseconds");
//            logger.log(Level.INFO, "This data size is " + sizeRDD);
//
//            rdd = next_rdd;
//            partitions = next_partitions;
//            runTime = runTime + 1;
//            //java.sql.Timestamp endTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
//
//
//            //System.out.println("This run started at " + startTimestamp);
//            //System.out.println("This run finished at: " + endTimestamp);
//        }
//    }
//
//    public void ddgen(JavaRDD<T> inputRDD, userTest<T> testFunc, userSplit<T> splitFunc, LogManager lm, FileHandler fh) {
//        dd_helper(inputRDD, 2, testFunc, splitFunc, lm, fh);
//    }
//
//}
//
