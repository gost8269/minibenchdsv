package su.dsv.benchmark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class MiniBenchDsvApp {

    private static final String[] SECURITY_IDS = {"AAPL", "MSFT", "GOOG", "AMZN", "TSLA"};
    private static final String[] EXCHANGES = {"NYSE", "NASDAQ", "LSE", "TSE", "FWB"};

    private static final ConcurrentLinkedQueue<FinancialRecord> data = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<FinancialRecord> workerQueueA = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<FinancialRecord> workerQueueB = new ConcurrentLinkedQueue<>();

    private double usedMemory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024 * 1024);

    private AtomicBoolean isRunning = new AtomicBoolean(true);

    private static final Logger logger = LogManager.getLogger(MiniBenchDsvApp.class.getName());

    //Variables have been adjusted for our hardware specification and use-case
    //20 minutes in ms
    private static final int BENCHMARK_LENGTH_IN_MS = 1_200_000;
    private static final long MAX_MESSAGES_IN_QUEUE = 200_000_000L;
    private static final long START_CONSUMING_MESSAGE_IN_QUEUE_SIZE = 1_000_000L;
    //How many messages per second can your application handle?
    private static final long MESSAGES_PER_SECOND = 50_000L;
    //How much does it cost the application?
    private static final long MESSAGE_PER_SECOND_LATENCY = 1000L;

    private static final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

    public void init() throws InterruptedException {

        long terminationTime = System.currentTimeMillis() + BENCHMARK_LENGTH_IN_MS;
        Instant instant = Instant.ofEpochMilli(terminationTime);
        logger.info("Benchmark started. Termination time will be: " + instant);

        logger.info("Benchmark settings: BENCHMARK_LENGTH_IN_MS="+BENCHMARK_LENGTH_IN_MS + '\n' +
                ", MAX_MESSAGES_IN_QUEUE="+MAX_MESSAGES_IN_QUEUE+ '\n' +
                ", START_CONSUMING_MESSAGE_IN_QUEUE_SIZE="+ START_CONSUMING_MESSAGE_IN_QUEUE_SIZE);

        logger.info("Used memory: " + usedMemory + " GB");

        new Thread(() -> {
            try {
                produceData();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, "producer-thread").start();

        new Thread(() -> {
            try {
                consumeData();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, "consumer-thread").start();

        new Thread(() -> {
            try {
                applicationWorker(workerQueueA, 1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, "worker-business-thread").start();

        new Thread(() -> {
            try {
                applicationWorker(workerQueueB, 2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, "worker-IO-thread").start();

        while (isRunning.get()) {
            if (System.currentTimeMillis() > terminationTime) {
                logger.info("Benchmark completed!");
                isRunning.set(false);
                break;
            }
            Thread.sleep(5000);
        }

    }

    private void applicationWorker(ConcurrentLinkedQueue<FinancialRecord> data, long calculationCostMultiplier) throws InterruptedException {
        int pollCount = 0;
        long messagesProcessed=0;

        while (isRunning.get()) {
            Object record = data.poll();

            if (record == null) {
                Thread.sleep(1);
                continue;
            }

            simulateThroughputCost(++pollCount, calculationCostMultiplier);
            messagesProcessed++;
        }

        logger.info("Messages processed end result:{}", messagesProcessed);
    }

    private void simulateThroughputCost(int pollCount, long calculationCostMultiplier) throws InterruptedException {
        //Don't want to spam the logging
        if(pollCount % (MESSAGES_PER_SECOND*10) == 0) {
            logger.info("Still polling messages and performing computational tasks. " +
                    "Messages pending={}", data.size());
            logger.info("CPU average load: " + osBean.getSystemLoadAverage()*100 +"%");
        }

        if (pollCount % MESSAGES_PER_SECOND == 0) {
            Thread.sleep(MESSAGE_PER_SECOND_LATENCY * calculationCostMultiplier);
        }

    }

    private void consumeData() throws InterruptedException {
        //When MAX RAM - offset, then continue poll section
        while (data.size() < START_CONSUMING_MESSAGE_IN_QUEUE_SIZE) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        logger.info("Dequeue started!");

        int pollCount = 0;

        while (isRunning.get()) {
            FinancialRecord record = data.poll();

            if(record == null) {
                Thread.sleep(1);
                continue;
            }

            simulateThroughputCost(++pollCount, 1);

            workerQueueA.add(record);
            workerQueueB.add(record);
        }

        logger.info("Thread finished");
    }

    private void produceData() throws InterruptedException {
        Random random = new Random();

        int addCount = 0;
        long dataProducerLatency=1;

        try {
            while (isRunning.get()) {
                FinancialRecord record = new FinancialRecord(
                        SECURITY_IDS[random.nextInt(SECURITY_IDS.length)],
                        EXCHANGES[random.nextInt(EXCHANGES.length)],
                        random.nextDouble() * 1000, // Price
                        random.nextInt(100_000) // Volume
                );
                data.add(record);

                addCount++;

                //Print progress and memory usage
                if (addCount % 10_000_000L == 0) {
                    addCount = 0;
                    logger.info("Number of messages pending=" + data.size());
                    usedMemory = (Runtime.getRuntime().totalMemory() -
                            Runtime.getRuntime().freeMemory()) / (1024 * 1024 * 1024);
                    logger.info("[NORMAL] Still producing data." + " Used memory: " + usedMemory + " GB");

                    if((data.size() > MAX_MESSAGES_IN_QUEUE)) {
                        logger.info("Reached MAX_MESSAGES_IN_QUEUE="+MAX_MESSAGES_IN_QUEUE +
                                ". Slowing down production of data.");
                        dataProducerLatency = 30_000; //Slowing it down by 10_000_000 records per 30 seconds
                    }

                    //Emulating a latency of 1 ms for each 10 000 000 record
                    Thread.sleep(dataProducerLatency);
                    }
                }

            } catch (OutOfMemoryError e) {
            logger.error("Benchmark reached out of memory:", e);
        }

        logger.info("Thread finished");
    }


    private record FinancialRecord(String securityId, String exchange, double price, int volume) { }
}
