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


import com.niara.logger.readers.FileReader;
import com.niara.logger.utils.LoggerConfig;
import com.niara.logger.utils.Tuple;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorCompletionService;

import static org.mockito.Mockito.*;

public class FileToLogReaderHandlerTest {

    @BeforeClass
    public void init() {
        LoggerConfig.init("TEST");
    }

    @Test
    public void testHandleWithValidFileName() {
        ExecutorCompletionService<Tuple<Long, String>> mockECS = mock(ExecutorCompletionService.class);
        when(mockECS.submit(Mockito.any(FileReader.class))).thenReturn(null);

        File tmpFile = null;
        try {
            // Valid filenames have a period(.) followed by a number for uniqueness
            tmpFile = File.createTempFile("logger-", "-reader-handler.1");
            tmpFile.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }
        Assert.assertNotNull(tmpFile);
        Path mockPath = mock(Path.class);
        when(mockPath.toFile()).thenReturn(tmpFile);

        FileToLogReaderHandler fileToLogReaderHandler = new FileToLogReaderHandler(mockECS);
        fileToLogReaderHandler.handle(mockPath);
        fileToLogReaderHandler.done();

        Mockito.verify(mockPath, Mockito.times(1)).toFile();
        Mockito.verify(mockECS, Mockito.times(1)).submit(Mockito.any(FileReader.class));
    }

    @Test
    public void testHandleWithInValidFileName() {
        ExecutorCompletionService<Tuple<Long, String>> mockECS = mock(ExecutorCompletionService.class);
        when(mockECS.submit(Mockito.any(FileReader.class))).thenReturn(null);

        File tmpFile = null;
        try {
            tmpFile = File.createTempFile("logger-", "-reader-handler");
            tmpFile.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }
        Assert.assertNotNull(tmpFile);
        Path mockPath = mock(Path.class);
        when(mockPath.toFile()).thenReturn(tmpFile);

        FileToLogReaderHandler fileToLogReaderHandler = new FileToLogReaderHandler(mockECS);
        fileToLogReaderHandler.handle(mockPath);

        Mockito.verify(mockPath, Mockito.times(1)).toFile();
        Mockito.verify(mockECS, Mockito.times(0)).submit(Mockito.any(FileReader.class));
    }

}