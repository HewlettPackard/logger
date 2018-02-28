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

package com.niara.logger.writers;

import com.niara.logger.utils.Output;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

public abstract class LogWriter implements Callable<LogWriterCounter> {

    protected final String inputLogType;
    protected final String outputLogType;
    protected final List<Output> parsedLogs;

    public LogWriter(final String inputLogType, final String outputLogType, List<Output> parsedLogs) throws Exception {
        this.inputLogType = inputLogType;
        this.outputLogType = outputLogType;
        this.parsedLogs = parsedLogs;
    }

    public final LogWriterCounter call() {

        LogWriterCounter counter = null;
        try {
            init();

            counter = write();

            close();
        } catch (Exception e) {

        }

        return counter;

    }

    protected abstract void init() throws Exception;

    protected abstract LogWriterCounter write() throws IOException;

    protected abstract void close() throws IOException;

}
