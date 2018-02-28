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


import com.niara.logger.utils.LoggerConfig;
import com.niara.logger.utils.Output;
import com.niara.logger.writers.FileWriter;
import com.niara.logger.writers.LogWriterCounter;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.concurrent.ExecutorCompletionService;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogParserToLogWriterHandlerTest {

    @BeforeClass
    public void init() {
        LoggerConfig.init("TEST");
    }

    @Test
    public void testWriteWithException() {
        ExecutorCompletionService<LogWriterCounter> mockECS = mock(ExecutorCompletionService.class);
        doThrow(Exception.class).when(mockECS).submit(Mockito.any(FileWriter.class));

        LogParserToLogWriter logParserToLogWriter = new LogParserToLogWriterHandler("dummy_input", "dummy_output", mockECS);
        logParserToLogWriter.start();
        logParserToLogWriter.write(new ArrayList<Output>());
        logParserToLogWriter.done();

        Mockito.verify(mockECS, Mockito.times(1)).submit(Mockito.any(FileWriter.class));
    }

    @Test
    public void testWriteWithNoException() {
        ExecutorCompletionService<LogWriterCounter> mockECS = mock(ExecutorCompletionService.class);
        when(mockECS.submit(Mockito.any(FileWriter.class))).thenReturn(null);

        LogParserToLogWriter logParserToLogWriter = new LogParserToLogWriterHandler("dummy_input", "dummy_output", mockECS);
        logParserToLogWriter.start();
        logParserToLogWriter.write(new ArrayList<Output>());
        logParserToLogWriter.done();

        Mockito.verify(mockECS, Mockito.times(1)).submit(Mockito.any(FileWriter.class));
    }

}