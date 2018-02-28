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

package com.niara.logger.apps;

import com.niara.logger.listeners.LogListener;
import com.niara.logger.parsers.LogParserCounter;
import com.niara.logger.handlers.*;
import com.niara.logger.monitors.LogParserMonitor;
import com.niara.logger.monitors.LogReaderMonitor;
import com.niara.logger.monitors.LogWriterMonitor;
import com.niara.logger.parsers.Parser;
import com.niara.logger.utils.GrokHandler;
import com.niara.logger.monitors.LogParserSemaphoreMonitor;
import com.niara.logger.utils.LoggerConfig;
import com.niara.logger.utils.Tuple;
import com.niara.logger.writers.LogWriterCounter;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;


public class Logger {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Logger.class);

    private static final CountDownLatch logListenerLatch = new CountDownLatch(1);
    private static final CountDownLatch logReaderLatch = new CountDownLatch(1);
    private static final CountDownLatch logParserLatch = new CountDownLatch(1);
    private static final CountDownLatch logWriterLatch = new CountDownLatch(1);
    private static AtomicLong atomicCounter = new AtomicLong(0);

    static {
        LoggerConfig.init("PRODUCTION");
        GrokHandler.init();
        Parser.init(LoggerConfig.getTimestampKeys(), LoggerConfig.getTimestampDefault());
    }

    public static AtomicLong getAtomicCounter() {
        return atomicCounter;
    }

    // LogReader
    private static final ExecutorService logReaderService = LoggerConfig.getLogReadersExecutorService();
    private static final ExecutorCompletionService<Tuple<Long, String>> logReaderECS = new ExecutorCompletionService<>(logReaderService);

    // LogParser
    private static final Semaphore logParserSemaphore = new Semaphore(LoggerConfig.getLogParsersQueueLength());
    private static final ExecutorService logParserService = LoggerConfig.getLogParsersExecutorService();
    private static final ExecutorCompletionService<LogParserCounter> logParserECS = new ExecutorCompletionService<>(logParserService);

    // LogWriter
    private static final ExecutorService logWriterService = LoggerConfig.getLogWritersExecutorService();
    private static final ExecutorCompletionService<LogWriterCounter> logWriterECS = new ExecutorCompletionService<>(logWriterService);


    public static ExecutorCompletionService<Tuple<Long, String>> getLogReaderECS() {
        return logReaderECS;
    }

    public static Semaphore getLogParserSemaphore() {
        return logParserSemaphore;
    }

    public static ExecutorCompletionService<LogParserCounter> getLogParserECS() {
        return logParserECS;
    }

    public static ExecutorCompletionService<LogWriterCounter> getLogWriterECS() {
        return logWriterECS;
    }

    private static void addShutdownHook(final CountDownLatch logListenerLatch,
                                        final FutureTask<Void> logListenerFuture,
                                        final CountDownLatch logReaderLatch,
                                        final ExecutorService logReaderService,
                                        final CountDownLatch logParserLatch,
                                        final ExecutorService logParserService,
                                        final CountDownLatch logWriterLatch,
                                        final ExecutorService logWriterService) {
        Runtime.getRuntime().addShutdownHook(
            new Thread(new ShutdownHandler(logListenerLatch, logListenerFuture, logReaderLatch, logReaderService, logParserLatch, logParserService, logWriterLatch, logWriterService))
        );
    }

    public static void main(String[] args) throws Exception {

        final FileToLogReaderHandler fileToLogReaderHandler = new FileToLogReaderHandler(getLogReaderECS());

        final Class<? extends LogListener> logListenerClass = (Class<? extends LogListener>) Logger.class.getClassLoader().loadClass(LoggerConfig.getLogListenerClass());
        Constructor<? extends LogListener> logListenerCtor = logListenerClass.getConstructor(CountDownLatch.class, FileToLogReaderHandler.class);
        LogListener logListener = logListenerCtor.newInstance(logListenerLatch, fileToLogReaderHandler);

        final FutureTask<Void> logListenerFuture = new FutureTask<>(logListener, null);

        // Add the shutdown hook now (after initializing all variables).
        addShutdownHook(logListenerLatch, logListenerFuture, logReaderLatch, logReaderService, logParserLatch, logParserService, logWriterLatch, logWriterService);

        // Start the thread to monitor file reader objects, and print stats.
        new Thread(new LogReaderMonitor(logReaderLatch, logReaderService, logReaderECS)).start();

        new Thread(new LogParserSemaphoreMonitor(logParserSemaphore, logParserLatch)).start();

        // Start the thread to monitor parser objects, and submit output to log writers.
        new Thread(new LogParserMonitor(logParserSemaphore, logParserLatch, logParserService, logParserECS)).start();

        // Start the thread to monitor writer objects, and print stats.
        new Thread(new LogWriterMonitor(logWriterLatch, logWriterService, logWriterECS)).start();

        // Start the listener and block for it to finish.
        logListenerFuture.run();

        // If the runtime logReaderToLogParser exits earlier, program won't reach here.
        if (logListenerLatch.getCount() != 0) {
            logger.warn("Listener died! Too many threads being created? Please check the system limit on number of processes/threads, and tune the thread parameters.");
        }

        logger.info("Directory watcher finished.");
    }

}

