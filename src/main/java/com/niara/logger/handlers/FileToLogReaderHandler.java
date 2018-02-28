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

import com.niara.logger.listeners.DirectoryWatcherListener;
import com.niara.logger.readers.FileReader;
import com.niara.logger.utils.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.ExecutorCompletionService;


public class FileToLogReaderHandler implements DirectoryWatcherListener.Handler {

    private static final Logger logger = LoggerFactory.getLogger(FileToLogReaderHandler.class);

    private final ExecutorCompletionService<Tuple<Long, String>> logReaderECS;

    public FileToLogReaderHandler(ExecutorCompletionService<Tuple<Long, String>> logReaderECS) {
        this.logReaderECS = logReaderECS;
    }

    private static String getLogType(String name) {
        // Extract log type from name.
        String type = null;
        int end = name.indexOf('.');
        if (end != -1)
            type = name.substring(0, end);

        return type;
    }

    public void done() {
        logger.info("Directory watcher finished.");
    }

    public void handle(Path path) {
        final File file = path.toFile();
        final String fileName = file.getName();
        final String logType = getLogType(fileName);

        boolean error = true;

        if (logType != null) {
            logger.debug("Processing log file {} of type {}", fileName, logType);
            this.logReaderECS.submit(new FileReader(logType, path));
            error = false;
        } else {
            logger.error("Could not get log type from file name {}.", fileName);
        }

        if (error) {
            logger.error("Error during processing file {}, deleting it.", fileName);
            file.delete();
        }
    }

}