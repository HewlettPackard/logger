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
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class FileWriterTest {

    private Path tmpDirectoryPath;
    private String origPath;

    @BeforeClass
    public void init() {
        LoggerConfig.init("TEST");
    }

    @BeforeMethod
    public void setUp() throws Exception {
        origPath = LoggerConfig.getLogWriterOutputDirectory();
        tmpDirectoryPath = Files.createTempDirectory("logger-writer");
    }

    @AfterMethod
    public void tearDown() {
        LoggerConfig.setLoggerWriterOutputDirectory(origPath);
        try {
            if (tmpDirectoryPath.toFile().exists()) {
                FileUtils.deleteDirectory(tmpDirectoryPath.toFile());
            }
        } catch (IOException e) {
            Assert.assertFalse(true, "Failed to delete tmpDirectory - " + tmpDirectoryPath.toString());
        }
    }

    @Test
    public void testWrite() throws Exception {
        LoggerConfig.setLoggerWriterOutputDirectory(tmpDirectoryPath.toString());
        List<Output> parsedLogs = new ArrayList<>();
        parsedLogs.add(new Output("{\"a\": \"A\"}", 1));
        parsedLogs.add(new Output("{\"b\": \"B\"}", 2));
        parsedLogs.add(new Output("{\"c\": \"C\"}", 3));
        FileWriter fileWriter = new FileWriter("dummy_input_type", "dummy_output_type", parsedLogs);
        LogWriterCounter logWriterCounter = fileWriter.call();

        Assert.assertEquals(logWriterCounter.getTotalCount(), 3);
        Assert.assertEquals(logWriterCounter.getSuccessCount(), 3);
        Assert.assertEquals(logWriterCounter.getErrorCount(), 0);
        Assert.assertEquals(logWriterCounter.getInputLogType(), "dummy_input_type");
        Assert.assertEquals(logWriterCounter.getOutputLogType(), "dummy_output_type");
    }

    @Test(expectedExceptions = {Exception.class}, expectedExceptionsMessageRegExp = ".* is not a valid output directory .*")
    public void testWriteWithInvalidDirectory() throws Exception {
        LoggerConfig.setLoggerWriterOutputDirectory(tmpDirectoryPath.toString());
        Assert.assertTrue(tmpDirectoryPath.toFile().delete());

        List<Output> parsedLogs = new ArrayList<>();
        parsedLogs.add(new Output("{\"a\": \"A\"}", 1));
        parsedLogs.add(new Output("{\"b\": \"B\"}", 2));
        parsedLogs.add(new Output("{\"c\": \"C\"}", 3));
        FileWriter fileWriter = new FileWriter("dummy_input_type", "dummy_output_type", parsedLogs);
        fileWriter.init();
    }

}
