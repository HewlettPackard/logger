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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class GrokHandler {

    static Logger logger = LoggerFactory.getLogger(GrokHandler.class);

    // TODO: Have a list of pattern directories in property file.
    private static String patternsDir = "/patterns";

    private static Pattern patternLine = Pattern.compile("(\\w+) (.*)");

    private static Map<String, String> grokPatterns = new HashMap<String, String>();

    private static Map<String, String> regexPatterns = new HashMap<String, String>();

    private GrokHandler() {
    }

    protected static Map<String, String> getGrokPatterns() {
        return grokPatterns;
    }

    static boolean load_patterns_file(BufferedReader br) throws IOException {
        boolean error = false;

        try {
            String line;
            while ((line = br.readLine()) != null) {
                // Ignore comment line.
                if (!line.isEmpty() && line.charAt(0) != '#') {
                    Matcher matcher = patternLine.matcher(line);
                    if (matcher.matches()) {
                        // TODO: Have a test mode to compile each pattern.
                        grokPatterns.put(matcher.group(1), matcher.group(2));
                    } else {
                        // logger.error("{}:{} Invalid pattern in grok file {}", file.getName(), lineCounter, line);
                        error = true;
                    }
                }
            }
        } finally {
            if (br != null) br.close();
        }

        return error;
    }

    private static class PatternFileVisitor extends SimpleFileVisitor<Path> {
        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attributes) {
            // logger.debug("Loading grok patterns file {}.", path);
            try {
                BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
                GrokHandler.load_patterns_file(reader);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return FileVisitResult.CONTINUE;
        }

        boolean error() {
            return false;
        }
    }

    private static boolean load_patterns_files() {
        try {
            URI uri = GrokHandler.class.getResource(patternsDir).toURI();
            Path myPath;
            if (uri.getScheme().equals("jar")) {
                FileSystem fileSystem = FileSystems.newFileSystem(uri, Collections.<String, Object>emptyMap());
                myPath = fileSystem.getPath(patternsDir);
            } else {
                myPath = Paths.get(uri);
            }

            PatternFileVisitor patternFileVisitor = new PatternFileVisitor();
            Files.walkFileTree(myPath, patternFileVisitor);
            return patternFileVisitor.error();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    private static String get_regex(String grokName) {
        String regex = regexPatterns.get(grokName);

        if (regex == null) {
            String grokPattern = grokPatterns.get(grokName);
            if (grokPattern != null) {
                logger.debug("Processing grok pattern named {} grok {}", grokName, grokPattern);
                regex = grok2regex(grokPattern);
                if (regex != null) {
                    regexPatterns.put(grokName, regex);
                } else {
                    logger.error("Could not convert grok pattern named {} value {} to regex.", grokName, grokPattern);
                }
            } else {
                logger.error("Grok pattern named {} not found.", grokName);
            }
        }

        return regex;
    }

    private static String process_grok_macro(String grok, int startOfMacro, int endOfMacro, int colon) {
        StringBuilder buffer = new StringBuilder();
        boolean error = false;

        if (colon != -1) {
            String patternName = grok.substring (startOfMacro + 2, colon);

            /*
            From http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html
            Group name

            A capturing group can also be assigned a "name", a named-capturing group, and then be back-referenced
            later by the "name". Group names are composed of the following characters. The first character must be a letter.

                The uppercase letters 'A' through 'Z' ('\u0041' through '\u005a'),
                The lowercase letters 'a' through 'z' ('\u0061' through '\u007a'),
                The digits '0' through '9' ('\u0030' through '\u0039'),

            A named-capturing group is still numbered as described in Group number.

            The captured input associated with a group is always the subsequence that the group most recently matched.
            If a group is evaluated a second time because of quantification then its previously-captured value,
            if any, will be retained if the second evaluation fails. Matching the string "aba" against the expression (a(b)?)+,
            for example, leaves group two set to "b". All captured input is discarded at the beginning of each match.

            Groups beginning with (? are either pure, non-capturing groups that do not capture text and
            do not count towards the group total, or named-capturing group.
           */
            // TODO: Do it under an option. Replacement string should be in property file.
            String captureName = grok.substring (colon + 1, endOfMacro);

            if (captureName.contains("_"))
                logger.info("Please do not use underscore in capture name:{}", captureName);

            if (captureName.contains("-"))
                logger.info("Please do not use hyphens in capture name:{}", captureName);

            if (captureName.contains(" ")) {
                logger.info("Please do not use spaces in capture name: {}", captureName);
            }

            captureName = captureName.replaceAll(" ", "");
            captureName = captureName.replaceAll ("_", "");
            captureName = captureName.replaceAll ("-", "");

            // (?<name>X)
            buffer.append("(?<").append(captureName).append('>');

            String regex = get_regex (patternName);
            if (regex != null)
                buffer.append(regex).append(')');
            else
                error = true;
        } else {
            String patternName = grok.substring (startOfMacro + 2, endOfMacro);
            String regex = get_regex (patternName);
            if (regex != null)
                buffer.append(regex);
            else
                error = true;
        }

        if (error)
            return null;
        else
            return buffer.toString();
    }

    /**
     * Returns error status.
     *
     * @return true if error, else false.
     */
    public static boolean init() {
        boolean error = load_patterns_files();

        if (error) {
            grokPatterns.clear();
            regexPatterns.clear();
        }

        return error;
    }

    public static String grok2regex(String grok) {
        logger.debug("Processing grok pattern {}", grok);

        StringBuilder buffer = new StringBuilder();

        boolean error = false;

        int endOfMacro = 0;
        int startOfMacro = grok.indexOf("%{");

        while (startOfMacro != -1) {
            buffer.append(grok.substring(endOfMacro, startOfMacro));

            endOfMacro = grok.indexOf('}', startOfMacro);
            if (endOfMacro == -1) {
                error = true;
                startOfMacro = -1;
            } else {
                // Generic pattern is %{PATTERN:NAME:TYPE}
                // TODO: Handle type
                int colon = grok.indexOf(':', startOfMacro);
                if (colon > endOfMacro)
                    colon = -1;
                String processed = process_grok_macro(grok, startOfMacro, endOfMacro, colon);
                if (processed != null) {
                    buffer.append(processed);
                    startOfMacro = grok.indexOf("%{", endOfMacro);
                    endOfMacro += 1;
                } else {
                    error = true;
                    startOfMacro = -1;
                }
            }
        }

        if (error) {
            return null;
        } else {
            buffer.append(grok.substring (endOfMacro));
            String regex = buffer.toString();
            return regex;
        }
    }

}
