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

package com.niara.logger.utils;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoggerConfig {

    private static final Logger logger = LoggerFactory.getLogger(LoggerConfig.class);

    private static String LOGGER_LISTENER_CLASS = "logger.listener.class";

    private static String LOGGER_LISTENER_INPUT_DIRECTORY = "logger.listener.input.directory";

    private static String LOGGER_LISTENER_POLL_INTERVAL = "logger.listener.poll.interval";

    private static String LOGGER_LISTENER_POLL_UNIT = "logger.listener.poll.unit";

    private static String LOGGER_LISTENER_HANDLER_CLASS = "logger.listener.handler.class";

    private static String LOGGER_READERS_POOL = "logger.readers.pool";

    private static String LOGGER_READER_MONITOR_POLL_INTERVAL = "logger.reader.monitor.poll.interval";

    private static String LOGGER_READER_MONITOR_POLL_UNIT = "logger.reader.monitor.poll.unit";

    private static String LOGGER_PARSERS_POOL = "logger.parsers.pool";

    private static String LOGGER_PARSERS_QUEUE_LENGTH = "logger.parsers.queue.length";

    private static String LOGGER_PARSERS_RATE_LIMIT_INTERVAL = "logger.parsers.logging.rate-limit.interval";

    private static String LOGGER_PARSERS_RATE_LIMIT_RATE = "logger.parsers.logging.rate-limit.rate";

    private static String LOGGER_PARSER_MONITOR_POLL_INTERVAL = "logger.parser.monitor.poll.interval";

    private static String LOGGER_PARSER_MONITOR_POLL_UNIT = "logger.parser.monitor.poll.unit";

    private static String LOGGER_PARSER_SEMAPHORE_MONITOR_POLL_INTERVAL = "logger.parser.semaphore.monitor.poll.interval";

    private static String LOGGER_PARSER_SEMAPHORE_MONITOR_POLL_UNIT = "logger.parser.semaphore.monitor.poll.unit";

    private static String LOGGER_WRITER_CLASS = "logger.writer.class";

    private static String LOGGER_WRITER_OUTPUT_DIRECTORY = "logger.writer.output.directory";

    private static String LOGGER_WRITERS_POOL = "logger.writers.pool";

    private static String LOGGER_WRITER_MONITOR_POLL_INTERVAL = "logger.writer.monitor.poll.interval";

    private static String LOGGER_WRITER_MONITOR_POLL_UNIT = "logger.writer.monitor.poll.unit";

    private static String LOGGER_OUTPUT_TIMESTAMP = "logger.output.timestamp";

    private static String LOGGER_OUTPUT_TIMESTAMP_DEFAULT = "logger.output.timestamp.default";

    private static String LOGGER_MAXLINES_PERBATCH = "logger.maxlines.perbatch";

    private static String LOGGER_PARSER_CONFIG_DIRECTORY = "logger.parser.config.directory";

    private static String LOGGER_OUTPUT_MAXIMUM_FILE_SIZE = "logger.output.maximum.file.size";

    private static String LOGGER_STATSD_HOST = "logger.statsd.host";

    private static String LOGGER_STATSD_PORT = "logger.statsd.port";

    private static String LOGGER_STATS_INTERVAL = "logger.stats.interval";

    private static Properties properties;

    private static String configDir = "config";

    private static String templateDir = "templates";

    private static List<JSONObject> configs = new ArrayList<>();

    private static Map<String, String> input2output = new HashMap<>();

    private static Map<String, String> input2description = new HashMap<>();

    private static Map<String, JSONObject> input2config = new HashMap<>();


    private static boolean isValidConfig(JSONObject config, String configFile) {
        boolean isValid = true;

        if (config != null) {
            // Validate config JSON for required fields
            if (config.get("input") == null) {
                logger.error("Required parameter: [input] missing from {} config file", configFile);
                isValid = false;
            }
            if (config.get("output") == null) {
                logger.error("Required parameter: [output] missing from {} config file", configFile);
                isValid = false;
            }
            // TODO: Add more validation
        } else {
            logger.error("Failed to load {} config file", configFile);
            isValid = false;
        }

        return isValid;
    }

    public static void loadConfigFiles(String directory) {
        List<String> configFiles = new ArrayList<>();

        loadFilesFromConfigDirectory(new File(getParserConfigDirectory() + "/" + directory), configFiles);

        if (configFiles != null) {
            for(String configFile : configFiles) {
                InputStream jsonResource = null;
                try {
                    jsonResource = new FileInputStream(new File(configFile));
                } catch (FileNotFoundException e) {
                    logger.error("FileNotFoundException", e);
                }
                JSONParser jsonConfigParser = new JSONParser();
                BufferedReader br = new BufferedReader(new InputStreamReader(jsonResource, StandardCharsets.UTF_8));
                JSONObject configJSON = null;
                try {
                    configJSON = (JSONObject) jsonConfigParser.parse(br);
                } catch (IOException ioError) {
                    logger.error("Error reading config {}", configFile);
                    logger.error("IOException", ioError);
                } catch (ParseException parseError) {
                    logger.error("Config {} not in valid JSON format", configFile);
                    logger.error("ParseException", parseError);
                }
                if (isValidConfig(configJSON, configFile)) {
                    configs.add(configJSON);
                    logger.info("Config {} file loaded successfully", configFile);
                }
                else {
                    logger.error("Invalid parsing config files found. Please fix the parsing configs and restart logger.");
                    System.exit(-1);
                }
            }
        }
    }

    public static void loadProductionConfigFiles() {
        loadConfigFiles(configDir);
        loadConfigFiles(templateDir);
    }

    private static void loadFilesFromConfigDirectory(File folder, List<String> fileNames) {
        File[] files = folder.listFiles();
        if (files == null) {
            logger.error("Missing directory - {}", folder.getAbsolutePath());
            return;
        }

        for (final File file : files) {
            if (file.isDirectory()) {
                loadFilesFromConfigDirectory(file, fileNames);
            } else {
                fileNames.add(file.getPath());
            }
        }
    }

    public static void loadTestConfigFiles() {
        String baseDir = new File("").getAbsolutePath();
        String relDir = "/src/main/resources/templates";
        File folder = new File(baseDir, relDir);
        List<String> fileNames = new ArrayList();
        loadFilesFromConfigDirectory(folder, fileNames);
        for (String fileName: fileNames) {
            String configFile = fileName.substring(fileName.lastIndexOf("/templates"));
            InputStream jsonResource = LoggerConfig.class.getResourceAsStream(configFile);
            JSONParser jsonConfigParser = new JSONParser();
            BufferedReader br = new BufferedReader(new InputStreamReader(jsonResource, StandardCharsets.UTF_8));
            JSONObject configJSON = null;
            try {
                configJSON = (JSONObject) jsonConfigParser.parse(br);
            } catch (IOException ioError) {
                logger.error("Error reading config {}", configFile);
                logger.error("IOException", ioError);
            } catch (ParseException parseError) {
                logger.error("Config {} not in valid JSON format", configFile);
                logger.error("ParseException", parseError);
            }
            if (isValidConfig(configJSON, configFile))
                configs.add(configJSON);
            else {
                logger.error("Invalid parsing config files found. Please fix the parsing configs and restart logger.");
                configs.add(null);
            }
        }
    }

    public static void init(String env) {
        properties = new Properties();
        try {
            // Load default property.
            InputStream in = LoggerConfig.class.getResourceAsStream("logger.properties");
            if (in == null)
                in = LoggerConfig.class.getClassLoader().getResourceAsStream("logger.properties");
            properties.load(in);

            // Now load override file from command line.
            String externalFileName = System.getProperty("logger.properties");
            if (externalFileName != null) {
                InputStream fin = new FileInputStream(new File(externalFileName));
                logger.info("Loading properties file {}", externalFileName);
                properties.load(fin);
                fin.close();
            }

            // load all JSON parsing config files
            if (env != null && env.equals("PRODUCTION"))
                loadProductionConfigFiles();
            else
                loadTestConfigFiles();

            loadLogInput2Configs();
        } catch (IOException e) {
            logger.error("Error during loading properties file", e);
            System.exit(-1);
        }
    }

    public static String get(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public static String getLogListenerClass() {
        return get(LOGGER_LISTENER_CLASS, null);
    }

    public static String getLogListenerInputDirectory() {
        return get(LOGGER_LISTENER_INPUT_DIRECTORY, null);
    }

    public static void setLogListenerInputDirectory(String inputDirectory) {
        properties.setProperty(LOGGER_LISTENER_INPUT_DIRECTORY, inputDirectory);
    }

    public static int getLogListenerPollInterval() {
        return Integer.parseInt(get(LOGGER_LISTENER_POLL_INTERVAL, null));
    }

    public static String getLogListenerPollUnit() {
        return get(LOGGER_LISTENER_POLL_UNIT, null);
    }

    public static String getLogListenerHandlerClass() {
        return get(LOGGER_LISTENER_HANDLER_CLASS, null);
    }

    public static String getLoggerWriterClass() {
        return get(LOGGER_WRITER_CLASS, null);
    }

    public static String getLogWriterOutputDirectory() {
        return get(LOGGER_WRITER_OUTPUT_DIRECTORY, null);
    }

    public static void setLoggerWriterOutputDirectory(String outputDirectory) {
        properties.setProperty(LOGGER_WRITER_OUTPUT_DIRECTORY, outputDirectory);
    }

    public static long getMaximumOutputFileSize() {
        return Long.parseLong(get(LOGGER_OUTPUT_MAXIMUM_FILE_SIZE, null));
    }

    public static int getLogReadersPool() {
        return Integer.parseInt(get(LOGGER_READERS_POOL, null));
    }

    public static int getLogReaderMonitorPollInterval() {
        return Integer.parseInt(get(LOGGER_READER_MONITOR_POLL_INTERVAL, null));
    }

    public static String getLogReaderMonitorPollUnit() {
        return get(LOGGER_READER_MONITOR_POLL_UNIT, null);
    }

    public static int getLogParsersPool() {
        return Integer.parseInt(get(LOGGER_PARSERS_POOL, null));
    }

    public static int getLogParsersQueueLength() {
        return Integer.parseInt(get(LOGGER_PARSERS_QUEUE_LENGTH, null));
    }

    public static int getLogParserMonitorPollInterval() {
        return Integer.parseInt(get(LOGGER_PARSER_MONITOR_POLL_INTERVAL, null));
    }

    public static String getLogParserMonitorPollUnit() {
        return get(LOGGER_PARSER_MONITOR_POLL_UNIT, null);
    }

    public static long getLogParserSemaphoreMonitorPollInterval() {
        return Long.parseLong(get(LOGGER_PARSER_SEMAPHORE_MONITOR_POLL_INTERVAL, null));
    }

    public static String getLogParserSemaphoreMonitorPollUnit() {
        return get(LOGGER_PARSER_SEMAPHORE_MONITOR_POLL_UNIT, null);
    }

    public static int getLogWritersPool() {
        return Integer.parseInt(get(LOGGER_WRITERS_POOL, null));
    }

    public static int getLogWriterMonitorPollInterval() {
        return Integer.parseInt(get(LOGGER_WRITER_MONITOR_POLL_INTERVAL, null));
    }

    public static String getLogWriterMonitorPollUnit() {
        return get(LOGGER_WRITER_MONITOR_POLL_UNIT, null);
    }

    public static String getStatsDHost() {
        return get(LOGGER_STATSD_HOST, null);
    }

    public static int getStatsDPort() {
        return Integer.parseInt(get(LOGGER_STATSD_PORT, null));
    }

    public static String getParserConfigDirectory() {
        return get(LOGGER_PARSER_CONFIG_DIRECTORY, null);
    }

    private static String[] getArray(final String string) {
        String[] array = string.split(",");
        int num = array.length;
        for (int i = 0; i < num; i++) {
            array[i] = array[i].trim();
        }

        return array;
    }

    private static ExecutorService getExecutorService(int nThreads) {
        return Executors.newFixedThreadPool(nThreads);
    }

    public static ExecutorService getLogParsersExecutorService() {
        return getExecutorService(LoggerConfig.getLogParsersPool());
    }

    public static ExecutorService getLogWritersExecutorService() {
        return getExecutorService(LoggerConfig.getLogWritersPool());
    }

    public static ExecutorService getLogReadersExecutorService() {
        return getExecutorService(LoggerConfig.getLogReadersPool());
    }

    public static long getTimestampDefault() {
        return Long.parseLong(get(LOGGER_OUTPUT_TIMESTAMP_DEFAULT, null));
    }

    public static String[] getTimestampKeys() {
        return getArray(get(LOGGER_OUTPUT_TIMESTAMP, ""));
    }

    public static int getMaxLinesPerBatch() { return Integer.parseInt(get(LOGGER_MAXLINES_PERBATCH, "")); }

    public static long getStatsInterval() {
        return Long.parseLong(get(LOGGER_STATS_INTERVAL, ""));
    }

    public static void setStatsInterval(String interval) {
        properties.setProperty(LOGGER_STATS_INTERVAL, interval);
    }

    public static long getLogParsersRateLimitInterval() {
        return Long.parseLong(get(LOGGER_PARSERS_RATE_LIMIT_INTERVAL, null));
    }

    public static int getLogParsersRateLimitRate() {
        return Integer.parseInt(get(LOGGER_PARSERS_RATE_LIMIT_RATE, null));
    }

    public static void loadLogInput2Configs() {
        for (JSONObject config: configs) {
            String input = (String) config.get("input");

            if (!input2output.containsKey(input))
                input2output.put(input, (String) config.get("output"));

            if (!input2description.containsKey(input))
                input2description.put(input, (String) config.get("description"));

            if (!input2config.containsKey(input))
                input2config.put(input, config);

        }
    }

    public static Map<String, String> getLogInput2Output() {
        return input2output;
    }

    public static String getLogInput2Output(String input) {
        return input2output.get(input);
    }

    public static Map<String, JSONObject> getLogInput2Config() {
        return input2config;
    }

    public static JSONObject getLogInput2Config(String logType) {
        return input2config.get(logType);
    }

}