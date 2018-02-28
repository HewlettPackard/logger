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

package com.niara.logger.listeners;

import com.niara.logger.handlers.FileToLogReaderHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public abstract class LogListener implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(DirectoryWatcherListener.class);

    protected final CountDownLatch logListenerLatch;
    protected final FileToLogReaderHandler handler;

    public LogListener(final CountDownLatch logListenerLatch, final FileToLogReaderHandler handler) {
       this.logListenerLatch = logListenerLatch;
       this.handler = handler;
    }

    public abstract void init() throws Exception;

    public abstract void processEvent() throws Exception;

    public abstract void close() throws Exception;

    public void run() {

        try {
            init();

            processEvent();

            close();
        } catch (Exception e) {
            logger.error("LogListener Exception: {}", e);
        }

    }

}
