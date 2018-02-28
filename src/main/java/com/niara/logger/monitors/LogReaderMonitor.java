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

package com.niara.logger.monitors;

import com.niara.logger.stats.LogCounter;
import com.niara.logger.stats.NonBlockingStatsDClient;
import com.niara.logger.utils.LoggerConfig;
import com.niara.logger.utils.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class LogReaderMonitor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(LogReaderMonitor.class);

    private final CountDownLatch logReaderLatch;
    private final ExecutorService logReaderService;
    private final ExecutorCompletionService<Tuple<Long, String>> logReaderECS;
    private final NonBlockingStatsDClient stats;

    public LogReaderMonitor(final CountDownLatch logReaderLatch,
                            final ExecutorService logReaderService,
                            final ExecutorCompletionService<Tuple<Long, String>> logReaderECS) {
        this.logReaderLatch = logReaderLatch;
        this.logReaderService = logReaderService;
        this.logReaderECS = logReaderECS;
        this.stats = NonBlockingStatsDClient.getTSDBStats();
    }

    public void run() {
        long count = 0;
        long previous = System.currentTimeMillis();
        boolean terminated;
        LogCounter logCounter = new LogCounter(stats);

        try {
            do {
                terminated = logReaderService.awaitTermination(LoggerConfig.getLogReaderMonitorPollInterval(), TimeUnit.valueOf(LoggerConfig.getLogReaderMonitorPollUnit().toUpperCase()));
                Future<Tuple<Long, String>> future;
                // Drain the queue as much as possible.
                while ((future = logReaderECS.poll()) != null) {
                    Tuple<Long, String> tuple = future.get();
                    count += tuple.x;
                    logCounter.updateReaderStats(tuple.x, tuple.y);
                    long now = System.currentTimeMillis();
                    if (stats != null) {
                        if (now - previous >= LoggerConfig.getStatsInterval()) {
                            logCounter.savePeriodicReaderStats();
                            logger.debug("LogReader read {} logs from {} to {}", count, previous, now);
                            count = 0;
                            previous = now;
                        }
                    }
                }
            } while (!terminated);

            if (!logReaderService.isTerminated()) {
                logger.error("LogReader is not terminated after shutdown.");
            }
        } catch (Exception e) {
            logger.error("LogReaderServiceException", e);
        }

        logger.info("LogReader service terminated, signalling that all log readers have finished.");

        if (stats != null) {
            stats.stop();
        }

        // Now signal that all file readers have shutdown. This is used during shutdown process.
        logReaderLatch.countDown();
    }

}