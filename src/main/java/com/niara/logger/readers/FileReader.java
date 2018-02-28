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

package com.niara.logger.readers;

import com.niara.logger.utils.LoggerConfig;
import com.niara.logger.utils.Tuple;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;


public class FileReader extends LogReader {

    private final Path path;

    public FileReader(final String inputType, final Path path) {
        super(inputType);
        this.path = path;
    }

    // ^\s*$ is an empty line.
    // ^\s*# is a commented line.
    private static boolean commentOrEmptyLine(final String line) {
        final int length = line.length();
        // Skip whitespace chars.
        int index = 0;
        for (; index < length && Character.isWhitespace(line.charAt(index)); index++) ;

        if (index < length) {
            if (line.charAt(index) == '#')
                // Char after whitespace(s) is '#', hence its a comment line.
                return true;
            else
                // Its a valid log line.
                return false;
        } else {
            // Empty line or just contains whitespace chars.
            return true;
        }
    }

    private long getNumLogsRead() throws Exception {
        File file = this.path.toFile();
        int count = 0;
        long totalCount = 0;

        BufferedReader br = null;
        int numLines = LoggerConfig.getMaxLinesPerBatch();
        ArrayList <String> lines = new ArrayList<>(numLines);
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
            String line;
            while ((line = br.readLine()) != null) {
                if (!commentOrEmptyLine(line)) {
                    lines.add(line);
                    count += 1;
                    totalCount += 1;
                    if (count == numLines) {
                        count = 0;
                        logReaderToLogParser.parse(lines);
                        lines = new ArrayList<>(numLines);
                    }
                }
            }
        } finally {
            if (count != 0) {
                logReaderToLogParser.parse(lines);
            }
            if (br != null) {
                br.close();
            }
        }

        file.delete();
        return totalCount;
    }

    @Override
    public Tuple<Long, String> read() throws Exception{
        logReaderToLogParser.start();
        long count = getNumLogsRead();
        logReaderToLogParser.done();
        return new Tuple<>(Long.valueOf(count), logType);
    }

}