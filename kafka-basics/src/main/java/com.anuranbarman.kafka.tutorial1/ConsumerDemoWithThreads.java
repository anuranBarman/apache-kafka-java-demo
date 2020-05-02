package com.anuranbarman.kafka.tutorial1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        String groupId = "my-sixth-application";
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(groupId,latch);

        Thread myConsumerThread = new Thread(myConsumerRunnable);
        myConsumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught Shutdown Hook");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted ",e);
        }finally {
            logger.info("Application is Closing");
        }

    }
}
