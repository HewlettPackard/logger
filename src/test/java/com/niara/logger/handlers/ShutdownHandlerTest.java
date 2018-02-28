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

import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

import static org.mockito.Mockito.*;

public class ShutdownHandlerTest {

    @Test
    public void testShutdownHandlerThrowingException() throws Exception {
        CountDownLatch mockLogListenerLatch = mock(CountDownLatch.class);
        doNothing().when(mockLogListenerLatch).countDown();

        FutureTask<Void> mockLogListenerFuture = mock(FutureTask.class);
        when(mockLogListenerFuture.get()).thenReturn(null);

        ExecutorService mockLogReaderService = mock(ExecutorService.class);
        doNothing().when(mockLogReaderService).shutdown();

        CountDownLatch mockLogReaderLatch = mock(CountDownLatch.class);
        doThrow(InterruptedException.class).when(mockLogReaderLatch).await();

        ExecutorService mockLogParserService = mock(ExecutorService.class);
        doNothing().when(mockLogParserService).shutdown();

        CountDownLatch mockLogParserLatch = mock(CountDownLatch.class);
        doNothing().when(mockLogParserLatch).await();

        ExecutorService mockLogWriterService = mock(ExecutorService.class);
        doNothing().when(mockLogWriterService).shutdown();

        CountDownLatch mockLogWriterLatch = mock(CountDownLatch.class);
        doNothing().when(mockLogWriterLatch).await();

        ShutdownHandler shutdownHandler = new ShutdownHandler(mockLogListenerLatch, mockLogListenerFuture, mockLogReaderLatch, mockLogReaderService, mockLogParserLatch, mockLogParserService, mockLogWriterLatch, mockLogWriterService);
        shutdownHandler.run();

        Mockito.verify(mockLogListenerLatch, Mockito.times(1)).countDown();
        Mockito.verify(mockLogListenerFuture, Mockito.times(1)).get();
        Mockito.verify(mockLogReaderService, Mockito.times(1)).shutdown();
        Mockito.verify(mockLogReaderLatch, Mockito.times(1)).await();
        Mockito.verify(mockLogParserService, Mockito.times(0)).shutdown();
        Mockito.verify(mockLogParserLatch, Mockito.times(0)).await();
        Mockito.verify(mockLogWriterService, Mockito.times(0)).shutdown();
        Mockito.verify(mockLogWriterLatch, Mockito.times(0)).await();
    }

    @Test
    public void testShutdownHandler() throws Exception {
        CountDownLatch mockLogListenerLatch = mock(CountDownLatch.class);
        doNothing().when(mockLogListenerLatch).countDown();

        FutureTask<Void> mockLogListenerFuture = mock(FutureTask.class);
        when(mockLogListenerFuture.get()).thenReturn(null);

        ExecutorService mockLogReaderService = mock(ExecutorService.class);
        doNothing().when(mockLogReaderService).shutdown();

        CountDownLatch mockLogReaderLatch = mock(CountDownLatch.class);
        doNothing().when(mockLogReaderLatch).await();

        ExecutorService mockLogParserService = mock(ExecutorService.class);
        doNothing().when(mockLogParserService).shutdown();

        CountDownLatch mockLogParserLatch = mock(CountDownLatch.class);
        doNothing().when(mockLogParserLatch).await();

        ExecutorService mockLogWriterService = mock(ExecutorService.class);
        doNothing().when(mockLogWriterService).shutdown();

        CountDownLatch mockLogWriterLatch = mock(CountDownLatch.class);
        doNothing().when(mockLogWriterLatch).await();

        ShutdownHandler shutdownHandler = new ShutdownHandler(mockLogListenerLatch, mockLogListenerFuture, mockLogReaderLatch, mockLogReaderService, mockLogParserLatch, mockLogParserService, mockLogWriterLatch, mockLogWriterService);
        shutdownHandler.run();

        Mockito.verify(mockLogListenerLatch, Mockito.times(1)).countDown();
        Mockito.verify(mockLogListenerFuture, Mockito.times(1)).get();
        Mockito.verify(mockLogReaderService, Mockito.times(1)).shutdown();
        Mockito.verify(mockLogReaderLatch, Mockito.times(1)).await();
        Mockito.verify(mockLogParserService, Mockito.times(1)).shutdown();
        Mockito.verify(mockLogParserLatch, Mockito.times(1)).await();
        Mockito.verify(mockLogWriterService, Mockito.times(1)).shutdown();
        Mockito.verify(mockLogWriterLatch, Mockito.times(1)).await();
    }

}
