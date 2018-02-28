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

import com.niara.logger.utils.LoggerConfig;
import com.niara.logger.utils.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class FileWriter extends LogWriter {

    private static final Logger logger = LoggerFactory.getLogger(FileWriter.class);

    private File file = null;
    private OutputStreamWriter fileWriter = null;

    public FileWriter(final String inputLogType, final String outputLogType, List<Output> parsedLogs) throws Exception{
        super(inputLogType, outputLogType, parsedLogs);
    }

    @Override
    protected LogWriterCounter write() throws IOException {
        LogWriterCounter logWriterCounter = new LogWriterCounter(inputLogType, outputLogType, parsedLogs.size());
        for (Output output : parsedLogs) {
            try {
                fileWriter.write((String) output.value());
                fileWriter.write("\n");
            } catch (IOException e) {
                logWriterCounter.incrementErrorCount();
            }
        }

        return logWriterCounter;
    }

    @Override
    protected void init() throws Exception {
        final String outputDirectoryName = LoggerConfig.getLogWriterOutputDirectory();
        final File outputDirectory = Paths.get(outputDirectoryName).toFile();
        if (!outputDirectory.isDirectory() || !Files.isWritable(outputDirectory.toPath())) {
            logger.error("{} is not a valid output directory or is not writable", outputDirectoryName);
            throw new Exception(outputDirectoryName + " is not a valid output directory or is not writable");
        }
        file = new File(outputDirectory, outputLogType + "_" + com.niara.logger.apps.Logger.getAtomicCounter().getAndIncrement());
        logger.info("Starting log file writer thread and writing to output file {}.", file.getName());
        fileWriter = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        logger.info("Shutting down log file writer thread and closing output file {}.", file.getName());
        if (fileWriter != null) {
            fileWriter.flush();
            fileWriter.close();
            fileWriter = null;
        }
    }
}
