/*
* (C) Copyright [2018] Hewlett Packard Enterprise Development LP.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.niara.logger.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class ShutdownHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ShutdownHandler.class);

    private final CountDownLatch logListenerLatch;
    private final CountDownLatch logReaderLatch;
    private final CountDownLatch logParserLatch;
    private final CountDownLatch logWriterLatch;

    private final FutureTask<Void> logListenerFuture;
    private final ExecutorService logReaderService;
    private final ExecutorService logParserService;
    private final ExecutorService logWriterService;

    public ShutdownHandler(final CountDownLatch logListenerLatch,
                           final FutureTask<Void> logListenerFuture,
                           final CountDownLatch logReaderLatch,
                           final ExecutorService logReaderService,
                           final CountDownLatch logParserLatch,
                           final ExecutorService logParserService,
                           final CountDownLatch logWriterLatch,
                           final ExecutorService logWriterService) {
        this.logListenerLatch = logListenerLatch;
        this.logListenerFuture = logListenerFuture;
        this.logReaderLatch = logReaderLatch;
        this.logReaderService = logReaderService;
        this.logParserLatch = logParserLatch;
        this.logParserService = logParserService;
        this.logWriterLatch = logWriterLatch;
        this.logWriterService = logWriterService;
    }

    public void run() {
        try {
            logger.info("Shutting down system.");

            // Signal to shutdown listener thread.
            logger.info("Signalling to listener to shutdown.");
            logListenerLatch.countDown();
            if (logListenerFuture != null) {
                logListenerFuture.get();
                logger.info("Listener shutdown complete.");
            }

            logger.info("Shutting down file reader service.");
            logReaderService.shutdown();

            // Wait for all file readers.
            logger.info("Waiting for file readers to shutdown.");
            logReaderLatch.await();

            logger.info("Shutting down parser service.");
            logParserService.shutdown();

            // Wait for all parsers.
            logger.info("Waiting for all parsers to shutdown.");
            logParserLatch.await();

            // All the parsers have submitted the parsed Output to log writers.
            logger.info("Shutting down writer service");
            logWriterService.shutdown();

            // Wait for all writers.
            logger.info("Waiting for all writers to shutdown.");
            logWriterLatch.await();

            logger.info("System shutdown complete.");
        } catch (Exception e) {
            logger.error("GlobalShutdownException", e);
        }
    }

}
