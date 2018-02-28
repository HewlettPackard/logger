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
import com.niara.logger.utils.LoggerConfig;
import org.apache.commons.io.FileUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class DirectoryWatcherListenerTest {

    private Path tmpDirectoryPath;
    private String origPath;

    @BeforeClass
    public void init() {
        LoggerConfig.init("TEST");
    }

    @BeforeMethod
    public void setUp() throws Exception {
        origPath = LoggerConfig.getLogListenerInputDirectory();
        tmpDirectoryPath = Files.createTempDirectory("logger-listener");
    }

    @AfterMethod
    public void tearDown() {
        LoggerConfig.setLogListenerInputDirectory(origPath);
        try {
            if (tmpDirectoryPath.toFile().exists()) {
                FileUtils.deleteDirectory(tmpDirectoryPath.toFile());
            }
        } catch (IOException e) {
            Assert.assertFalse(true, "Failed to delete tmpDirectory - " + tmpDirectoryPath.toString());
        }
    }

    @Test(expectedExceptions = {Exception.class}, expectedExceptionsMessageRegExp = ".* is not a valid input directory .*")
    public void testDirectoryWatcherListenerWithInvalidDirectory() throws Exception {
        LoggerConfig.setLogListenerInputDirectory(tmpDirectoryPath.toString());

        DirectoryWatcherListener directoryWatcherListener = new DirectoryWatcherListener(null, null);
        Assert.assertTrue(tmpDirectoryPath.toFile().delete());
        directoryWatcherListener.init();
    }

    @Test
    public void testProcessEventWithOnlyExistingFiles() throws Exception {
        LoggerConfig.setLogListenerInputDirectory(tmpDirectoryPath.toString());

        FileToLogReaderHandler mockHandler = mock(FileToLogReaderHandler.class);
        doNothing().when(mockHandler).handle(Mockito.any(Path.class));

        CountDownLatch mockLatch = mock(CountDownLatch.class);
        when(mockLatch.getCount()).thenReturn( 0L);

        // Create existing files
        try {
            for (int i = 0; i < 3; i++) {
                OutputStreamWriter fileWriter = new OutputStreamWriter(new FileOutputStream(tmpDirectoryPath + File.separator + "dummy_log_type." + (i + 1)), StandardCharsets.UTF_8);
                fileWriter.write("This is a sample test log line");
                fileWriter.write("\n");
                fileWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }

        DirectoryWatcherListener directoryWatcherListener = new DirectoryWatcherListener(mockLatch, mockHandler);
        directoryWatcherListener.processEvent();

        Mockito.verify(mockLatch, Mockito.times(1)).getCount();
        Mockito.verify(mockHandler, Mockito.times(3)).handle(Mockito.any(Path.class));
    }

    @Test
    public void testProcessEventWithOnlyIncomingFilesWithValidWatchKey() throws Exception {
        LoggerConfig.setLogListenerInputDirectory(tmpDirectoryPath.toString());

        FileToLogReaderHandler mockHandler = mock(FileToLogReaderHandler.class);
        doNothing().when(mockHandler).handle(Mockito.any(Path.class));

        CountDownLatch mockLatch = mock(CountDownLatch.class);
        when(mockLatch.getCount()).thenReturn(1L).thenReturn(0L);

        try {
            OutputStreamWriter fileWriter = new OutputStreamWriter(new FileOutputStream(tmpDirectoryPath + File.separator + "dummy_log_type.0"), StandardCharsets.UTF_8);
            fileWriter.write("This is a sample test log line");
            fileWriter.write("\n");
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }
        WatchEvent.Kind mockEventKind = StandardWatchEventKinds.ENTRY_CREATE;
        WatchEvent<?> mockWatchEvent = mock(WatchEvent.class);
        when(mockWatchEvent.kind()).thenReturn(mockEventKind);
        Path path = Paths.get(tmpDirectoryPath + File.separator + "dummy_log_type.0");
        when(mockWatchEvent.context()).thenReturn(path);
        List<WatchEvent<?>> watchEvents = new LinkedList<>();
        watchEvents.add(mockWatchEvent);
        WatchKey mockWatchKey = mock(WatchKey.class);
        when(mockWatchKey.pollEvents()).thenReturn(watchEvents);
        when(mockWatchKey.reset()).thenReturn(true);

        WatchService mockWatchService = mock(WatchService.class);
        when(mockWatchService.poll(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(mockWatchKey);

        Path mockPath = mock(Path.class);
        when(mockPath.register(Mockito.any(WatchService.class), (WatchEvent.Kind<?>) Mockito.any())).thenReturn(mockWatchKey);

        DirectoryWatcherListener directoryWatcherListener = new DirectoryWatcherListener(mockLatch, mockHandler);
        directoryWatcherListener.setWatchService(mockWatchService);
        directoryWatcherListener.createAndGetWatchKey(mockPath);
        directoryWatcherListener.processEvent();

        Mockito.verify(mockLatch, Mockito.atLeast(2)).getCount();
        Mockito.verify(mockHandler, Mockito.times(2)).handle(Mockito.any(Path.class));
    }

    @Test
    public void testProcessEventWithOnlyIncomingFilesWithOverflowWatchEvent() throws Exception {
        LoggerConfig.setLogListenerInputDirectory(tmpDirectoryPath.toString());

        FileToLogReaderHandler mockHandler = mock(FileToLogReaderHandler.class);
        doNothing().when(mockHandler).handle(Mockito.any(Path.class));

        CountDownLatch mockLatch = mock(CountDownLatch.class);
        when(mockLatch.getCount()).thenReturn(1L).thenReturn(0L);

        WatchEvent.Kind mockEventKind = StandardWatchEventKinds.OVERFLOW;
        WatchEvent<?> mockWatchEvent = mock(WatchEvent.class);
        when(mockWatchEvent.kind()).thenReturn(mockEventKind);
        List<WatchEvent<?>> watchEvents = new LinkedList<>();
        watchEvents.add(mockWatchEvent);
        WatchKey mockWatchKey = mock(WatchKey.class);
        when(mockWatchKey.pollEvents()).thenReturn(watchEvents);
        when(mockWatchKey.reset()).thenReturn(true);

        WatchService mockWatchService = mock(WatchService.class);
        when(mockWatchService.poll(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(mockWatchKey);

        Path mockPath = mock(Path.class);
        when(mockPath.register(Mockito.any(WatchService.class), (WatchEvent.Kind<?>) Mockito.any())).thenReturn(mockWatchKey);

        DirectoryWatcherListener directoryWatcherListener = new DirectoryWatcherListener(mockLatch, mockHandler);
        directoryWatcherListener.setWatchService(mockWatchService);
        directoryWatcherListener.createAndGetWatchKey(mockPath);
        directoryWatcherListener.processEvent();

        Mockito.verify(mockLatch, Mockito.atLeast(2)).getCount();
        Mockito.verify(mockHandler, Mockito.times(0)).handle(Mockito.any(Path.class));
    }

    @Test
    public void testProcessEventWithOnlyIncomingFilesAndNullWatchEventContext() throws Exception {
        LoggerConfig.setLogListenerInputDirectory(tmpDirectoryPath.toString());

        FileToLogReaderHandler mockHandler = mock(FileToLogReaderHandler.class);
        doNothing().when(mockHandler).handle(Mockito.any(Path.class));

        CountDownLatch mockLatch = mock(CountDownLatch.class);
        when(mockLatch.getCount()).thenReturn(1L).thenReturn(0L);

        WatchEvent<?> mockWatchEvent = mock(WatchEvent.class);
        when(mockWatchEvent.kind()).thenReturn(null);
        List<WatchEvent<?>> watchEvents = new LinkedList<>();
        watchEvents.add(mockWatchEvent);
        WatchKey mockWatchKey = mock(WatchKey.class);
        when(mockWatchKey.pollEvents()).thenReturn(watchEvents);
        when(mockWatchKey.reset()).thenReturn(true);

        WatchService mockWatchService = mock(WatchService.class);
        when(mockWatchService.poll(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(mockWatchKey);

        Path mockPath = mock(Path.class);
        when(mockPath.register(Mockito.any(WatchService.class), (WatchEvent.Kind<?>) Mockito.any())).thenReturn(mockWatchKey);

        DirectoryWatcherListener directoryWatcherListener = new DirectoryWatcherListener(mockLatch, mockHandler);
        directoryWatcherListener.setWatchService(mockWatchService);
        directoryWatcherListener.createAndGetWatchKey(mockPath);
        directoryWatcherListener.processEvent();

        Mockito.verify(mockLatch, Mockito.atLeast(2)).getCount();
        Mockito.verify(mockHandler, Mockito.times(0)).handle(Mockito.any(Path.class));
    }

    @Test
    public void testProcessEventWithOnlyIncomingFilesWithInValidWatchKey() throws Exception {
        LoggerConfig.setLogListenerInputDirectory(tmpDirectoryPath.toString());

        FileToLogReaderHandler mockHandler = mock(FileToLogReaderHandler.class);
        doNothing().when(mockHandler).handle(Mockito.any(Path.class));

        CountDownLatch mockLatch = mock(CountDownLatch.class);
        when(mockLatch.getCount()).thenReturn(1L).thenReturn(0L);

        WatchKey mockWatchKey = mock(WatchKey.class);
        when(mockWatchKey.reset()).thenReturn(false);

        WatchService mockWatchService = mock(WatchService.class);
        when(mockWatchService.poll(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(mockWatchKey);

        Path mockPath = mock(Path.class);
        when(mockPath.register(Mockito.any(WatchService.class), (WatchEvent.Kind<?>) Mockito.any())).thenReturn(mockWatchKey);

        DirectoryWatcherListener directoryWatcherListener = new DirectoryWatcherListener(mockLatch, mockHandler);
        directoryWatcherListener.setWatchService(mockWatchService);
        directoryWatcherListener.createAndGetWatchKey(mockPath);
        directoryWatcherListener.processEvent();

        Mockito.verify(mockLatch, Mockito.atLeast(1)).getCount();
        Mockito.verify(mockHandler, Mockito.times(0)).handle(Mockito.any(Path.class));
    }

    @Test
    public void testClose() throws Exception {
        LoggerConfig.setLogListenerInputDirectory(tmpDirectoryPath.toString());

        FileToLogReaderHandler mockHandler = mock(FileToLogReaderHandler.class);
        doNothing().when(mockHandler).done();

        CountDownLatch mockLatch = mock(CountDownLatch.class);

        WatchKey mockWatchKey = mock(WatchKey.class);

        WatchService mockWatchService = mock(WatchService.class);
        doNothing().when(mockWatchService).close();

        Path mockPath = mock(Path.class);
        when(mockPath.register(Mockito.any(WatchService.class), (WatchEvent.Kind<?>) Mockito.any())).thenReturn(mockWatchKey);

        DirectoryWatcherListener directoryWatcherListener = new DirectoryWatcherListener(mockLatch, mockHandler);
        directoryWatcherListener.setWatchService(mockWatchService);
        directoryWatcherListener.createAndGetWatchKey(mockPath);
        directoryWatcherListener.close();

        Mockito.verify(mockHandler, Mockito.times(1)).done();
        Mockito.verify(mockWatchService, Mockito.times(1)).close();
    }

    @Test
    public void testCloseWithException() throws Exception {
        LoggerConfig.setLogListenerInputDirectory(tmpDirectoryPath.toString());

        FileToLogReaderHandler mockHandler = mock(FileToLogReaderHandler.class);
        doNothing().when(mockHandler).done();

        CountDownLatch mockLatch = mock(CountDownLatch.class);

        WatchKey mockWatchKey = mock(WatchKey.class);

        WatchService mockWatchService = mock(WatchService.class);
        doThrow(IOException.class).when(mockWatchService).close();

        Path mockPath = mock(Path.class);
        when(mockPath.register(Mockito.any(WatchService.class), (WatchEvent.Kind<?>) Mockito.any())).thenReturn(mockWatchKey);

        DirectoryWatcherListener directoryWatcherListener = new DirectoryWatcherListener(mockLatch, mockHandler);
        directoryWatcherListener.setWatchService(mockWatchService);
        directoryWatcherListener.createAndGetWatchKey(mockPath);
        directoryWatcherListener.close();

        Mockito.verify(mockHandler, Mockito.times(1)).done();
        Mockito.verify(mockWatchService, Mockito.times(1)).close();
    }

}