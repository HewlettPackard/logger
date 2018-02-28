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
import com.niara.logger.utils.FileTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;


public class DirectoryWatcherListener extends LogListener {

    public interface Handler {
        // Callback per file.
        void handle(final Path file);
        // Callback after shutting down the service.
        void done();
    }

    private static Logger logger = LoggerFactory.getLogger(DirectoryWatcherListener.class);

    private WatchService watchService;

    private static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }

    public void setWatchService(WatchService watchService) {
        this.watchService = watchService;
    }

    public WatchKey createAndGetWatchKey(Path path) throws Exception {
        return path.register(this.watchService, ENTRY_CREATE);
    }

    public DirectoryWatcherListener(final CountDownLatch logListenerLatch, final FileToLogReaderHandler handler) throws Exception {
        super(logListenerLatch, handler);
    }

    private Map<String, Long> readExistingFiles() throws InterruptedException {
        logger.info("Processing existing files.");
        Map<String, Long> existingFileNameToLastModifiedTime = new HashMap<>();

        logger.info("Listing directory.");

        // Read existing files in the directory.
        final File[] files = Paths.get(LoggerConfig.getLogListenerInputDirectory()).toFile().listFiles();
        final FileTime[] files2sort = new FileTime[files.length];
        for (int i = 0; i < files.length; i++)
            files2sort[i] = new FileTime(files[i]);

        // Now sort the files by last modified (oldest first).
        Arrays.sort(files2sort);

        // Process the files now.
        for (final FileTime ft : files2sort) {
            final File file = ft.getFile();
            logger.info("Directory watcher picked up existing file {}", file.getName());
            handler.handle(file.toPath());
            logger.info("Directory watcher handled existing file {}", file.getName());
            existingFileNameToLastModifiedTime.put(file.getName(), ft.getLastModified());
        }

        logger.info("Done processing existing files.");

        return existingFileNameToLastModifiedTime;
    }

    private void readIncomingFiles(Map<String, Long> existingFileNameToLastModifiedTime) throws InterruptedException {
        while (logListenerLatch.getCount() != 0) {
            WatchKey key = watchService.poll(LoggerConfig.getLogListenerPollInterval(), TimeUnit.valueOf(LoggerConfig.getLogListenerPollUnit().toUpperCase()));
            if (key != null) {
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind kind = event.kind();

                    if (kind == OVERFLOW) {
                        logger.error("Ignoring overflow event in DirectoryWatcherListener.");
                        continue;
                    }

                    WatchEvent<Path> ev = cast(event);
                    Path name = ev.context();
                    if (name != null) {
                        Path child = Paths.get(LoggerConfig.getLogListenerInputDirectory()).resolve(name);
                        FileTime currFile = new FileTime(child.toFile());

                        if (!existingFileNameToLastModifiedTime.containsKey(name.toString()) ||
                            (existingFileNameToLastModifiedTime.get(name.toString()) < currFile.getLastModified())) {
                            if (kind == ENTRY_CREATE) {
                                logger.info("Directory watcher received file {}", name);
                                handler.handle(child);
                                logger.info("Directory watcher handled file {}", name);
                            }
                        } else {
                            logger.error("Directory watcher failed to process duplicate file - {}", name);
                        }
                    } else {
                        logger.error("Directory watcher received null context.");
                    }
                }

                boolean valid = key.reset();
                if (!valid) {
                    logger.error("Watcher is not valid.");
                    break;
                }
            }
        }
    }

    private void validateInputDirectory(Path dir) throws Exception {
        if (dir == null || !Files.isDirectory(dir) || !Files.isReadable(dir)) {
            logger.error("{} is not a valid input directory or is not readable", dir);
            throw new Exception(dir + " is not a valid input directory or is not readable");
        }
    }

    @Override
    public void init() throws Exception {
        validateInputDirectory(Paths.get(LoggerConfig.getLogListenerInputDirectory()));
        setWatchService(FileSystems.getDefault().newWatchService());
        WatchKey watchKey = createAndGetWatchKey(Paths.get(LoggerConfig.getLogListenerInputDirectory()));
        logger.info("Watching directory: {}", watchKey.watchable());
    }

    @Override
    public void processEvent() throws  Exception {
        readIncomingFiles(readExistingFiles());
    }

    @Override
    public void close() throws Exception {
        logger.info("Shutting down directory watcher.");
        try {
            this.watchService.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        handler.done();
    }

}
