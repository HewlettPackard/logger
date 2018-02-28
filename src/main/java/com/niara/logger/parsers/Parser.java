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

package com.niara.logger.parsers;

import com.niara.logger.utils.Output;
import com.niara.logger.utils.GrokHandler;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public abstract class Parser {

    protected static Logger logger = LoggerFactory.getLogger(Parser.class);

    protected static final char HIERARCHICAL_SEPARATOR = ' ';
    protected static final char HIERARCHICAL_SEPARATOR_REPLACEMENT = '_';
    protected static String HIERARCHICAL_SEPARATOR_SPLIT = "" + HIERARCHICAL_SEPARATOR;

    private static long timestampDefault = 0;
    // Set of keys to use to find timestamp field in parsed event object.
    // This list is populated from the property file.
    private static String[] timestampKeys;

    private static final ConcurrentMap<String, Pattern> compiledPatterns = new ConcurrentHashMap<String, Pattern>();

    // The log line to parse.
    protected final String log;
    // Parsed tokens.
    protected Map<String, Object> event = null;
    // There is no way to get the names of captured patterns, so have to keep the matcher object around to get the matched value.
    protected Matcher topMatcher = null;

    // Used for testing
    public void setEvent(Map<String, Object> event) {
        this.event = event;
    }

    // Used for testing
    public void setTopMatcher(Matcher topMatcher) {
        this.topMatcher = topMatcher;
    }

    public static void init(final String[] timestampKeys,
                            final long timestampDefault) {
        Parser.timestampKeys = timestampKeys;
        Parser.timestampDefault = timestampDefault;
    }

    protected static Pattern grok2pattern(final String grok) {
        Pattern pattern = compiledPatterns.get(grok);
        if (pattern == null) {
            synchronized (compiledPatterns) {
                pattern = compiledPatterns.get(grok);
                if (pattern == null) {
                    final String regex = GrokHandler.grok2regex(grok);
                    if (regex != null) {
                        pattern = Pattern.compile(regex);
                        logger.debug("grok {} regex {}", grok, regex);
                        compiledPatterns.put(grok, pattern);
                        return pattern;
                    }
                }
            }
        }

        return pattern;
    }

    protected static List<String> getNamedGroups(final String grok) {
        List<String> namedGroups = null;
        final String regex = GrokHandler.grok2regex(grok);
        if (regex != null) {
            namedGroups = new ArrayList<String>();
            Matcher m = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>").matcher(regex);
            while (m.find()) {
                namedGroups.add(m.group(1));
            }

        }
        return namedGroups;
    }

    protected Parser(final String log) {
        this.log = log;
    }

    protected void copy(Map<String, Object> eventMap) {
        if (event == null)
            event = new HashMap<>();
        event.putAll(eventMap);
    }

    protected void copy(Matcher matcher) {
        topMatcher = matcher;
    }

    protected Map<String, Object> getEventMap() {
        return event;
    }

    protected Matcher getTopMatcher() {
        return topMatcher;
    }

    protected long getTimestamp() {
        for (final String key: Parser.timestampKeys) {
            final Object value = get_value(key);
            if (value != null) {
                if (value instanceof Long) {
                    return (Long) value;
                } else if (value instanceof String) {
                    try {
                        Long longValue = Long.parseLong((String) value);
                        return longValue;
                    } catch (NumberFormatException nfe) {
                        logger.debug("Cannot convert {} to long", value);
                        continue;
                    }
                }
            }
        }

        return Parser.timestampDefault;
    }

    protected Map<String, Object> get_last_map(final String[] keys) {
        Map<String, Object> tmp = event;
        for (int i = 0; i < keys.length - 1; i++) {
            Object obj = tmp.get(keys[i]);
            if (obj == null) {
                Map<String, Object> v = new HashMap<String, Object>();
                tmp.put(keys[i], v);
                tmp = v;
            } else if (obj instanceof Map) {
                tmp = (Map<String, Object>) obj;
            } else {
                tmp = null;
                break;
            }
        }

        return tmp;
    }

    protected Map<String, Object> get_last_map_discard(final String keys[]) {
        Map<String, Object> tmp = event;
        for (int i = 0; i < keys.length - 1; i++) {
            tmp = (Map<String, Object>) tmp.get(keys[i]);
            if (!(tmp instanceof Map)) {
                break;
            }
        }

        if (tmp instanceof Map) {
            return tmp;
        }

        return null;
    }

    public Object get_value(final String sourceKey) {
        Object value = get_value_simple(sourceKey);

        if (topMatcher != null) {
            final String[] keys = sourceKey.split(Pattern.quote(HIERARCHICAL_SEPARATOR_SPLIT));
            if (keys.length == 1) {
                value = event.get(sourceKey);
            } else {
                final Map<String, Object> tmp = get_last_map(keys);
                if (tmp != null)
                    value = tmp.get(keys[keys.length - 1]);
            }
        }

        return value;
    }

    public Object get_value_simple(final String sourceKey) {
        Object value = null;
        if (event != null) {
            value = event.get(sourceKey);
            if (value == null && topMatcher != null) {
                try {
                    value = topMatcher.group(sourceKey);
                } catch (IllegalStateException | IllegalArgumentException e) {
                    logger.debug("{}", e);
                }
            }
        }
        return value;
    }

    public void remove_key(final String sourceKey) {
        final String[] keys = sourceKey.split(Pattern.quote(HIERARCHICAL_SEPARATOR_SPLIT));
        if (keys.length == 1)
            event.remove(sourceKey);
        else {
            final Map<String, Object> tmp = get_last_map(keys);
            tmp.remove(keys[keys.length - 1]);
        }

    }

    protected static String get_unquoted_string(final String buffer,
                                              final int beginIndex,
                                              final int endIndex) {
        if (beginIndex != endIndex) {
            final char firstChar = buffer.charAt(beginIndex);
            final char lastChar = buffer.charAt(endIndex - 1);
            if ((firstChar == '"' || firstChar == '\'') && (firstChar == lastChar)) {
                return buffer.substring(beginIndex + 1, endIndex - 1);
            }
        }

        return buffer.substring(beginIndex, endIndex);
    }

    protected static void add_kv(final String kv,
                               final String value_split,
                               final int start,
                               final int end,
                               final Map<String, Object> map,
                               String keyPrefix) {
        final int kvIndex = kv.indexOf(value_split, start);
        if (kvIndex > start && kvIndex < end) {
            map.put(keyPrefix + kv.substring(start, kvIndex).trim(), get_unquoted_string(kv, kvIndex + 1, end));
        } else {
            // logger.debug("No kv in substring {}", kv.substring(start, end));
        }
    }

    // Limit using 'event' object in few methods only.
    protected Parser set_value_simple(final String sourceKey, final Object value) {
        if (event != null)
            event.put(sourceKey, value);

        return this;
    }

    // TODO: check if this can be made protected
    public Parser set_value(final String sourceKey, final Object value) {
        if (topMatcher != null) {
            final String[] keys = sourceKey.split(HIERARCHICAL_SEPARATOR_SPLIT);
            final Map<String, Object> tmp = get_last_map(keys);
            tmp.put(keys[keys.length - 1], value);
        }

        return this;
    }

    public Parser set_value(final String sourceKey, final Object value, boolean array, String content_type) {
        if (array) {
            if (topMatcher != null) {
                final String[] keys = sourceKey.split(HIERARCHICAL_SEPARATOR_SPLIT);
                final Map<String, Object> tmp = get_last_map(keys);
                if (tmp.get(keys[keys.length - 1]) == null) {
                    if (content_type.equals("integer")) {
                        tmp.put(keys[keys.length - 1], new ArrayList<Integer>());
                    } else if (content_type.equals("string")) {
                        tmp.put(keys[keys.length - 1], new ArrayList<String>());
                    } else if (content_type.equals("object")) {
                        tmp.put(keys[keys.length - 1], new ArrayList<Map<String, Object>>());
                    }

                }
                List <Object> targetArray = (List<Object>) tmp.get(keys[keys.length - 1]);
                targetArray.add(value);
            }
        }
        return this;
    }

    private static void to_json_impl(final Object object, final StringBuilder buffer) {
        if (object instanceof String) {
            String stringValue = (String) object;
            buffer.append('"');

            if (stringValue != null) {
                int length = stringValue.length();
                for (int i = 0; i < length; i++) {
                    if (stringValue.charAt(i) == '\\')
                        buffer.append("\\\\");
                    else if (stringValue.charAt(i) == '\"')
                        buffer.append("\\\"");
                    else if (stringValue.charAt(i) == '\n')
                        buffer.append("\\n");
                    else if (stringValue.charAt(i) == '\t')
                        buffer.append("\\t");
                    else if (stringValue.charAt(i) == '\r')
                        buffer.append("\\r");
                    else
                        buffer.append(stringValue.charAt(i));
                }
            }

            buffer.append('"');
        } else if (object instanceof Long) {
            buffer.append(((Long) object).longValue());
        } else if (object instanceof Integer) {
            buffer.append(((Integer) object).intValue());
        } else if (object instanceof Map) {
            final Map<String, Object> map = (Map<String, Object>) object;
            buffer.append('{');

            boolean isFirst = true;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (!isFirst)
                    buffer.append(',');
                buffer.append('"').append(entry.getKey()).append("\":");
                to_json_impl(entry.getValue(), buffer);
                isFirst = false;
            }

            buffer.append('}');
        } else if (object instanceof List) {
            final List<Object> list = (List<Object>) object;
            buffer.append('[');

            if (!list.isEmpty()) {
                boolean isFirst = true;
                for (Object v : list) {
                    if (!isFirst)
                        buffer.append(',');
                    to_json_impl(v, buffer);
                    isFirst = false;
                }
            }

            buffer.append(']');
        } else if (object instanceof Boolean) {
            buffer.append(((Boolean) object).booleanValue());
        } else if (object == null) {
            buffer.append(object);
        } else {
            // TODO: Handle Date object differently
            buffer.append('"').append(String.valueOf(object)).append('"');
        }
    }

    public Output to_json() {
        if (topMatcher != null) {
            final StringBuilder buffer = new StringBuilder();
            to_json_impl(event, buffer);
            return new Output(buffer.toString(), getTimestamp());
        } else {
            return new Output(null, 0);
        }
    }

    // Keep only specified list of keys in the event, others are removed.
    public Parser project(final List<String> keys) {
        if (topMatcher != null) {
            final Map<String, Object> currentEvent = event;
            // Create new map to store event.
            event = new HashMap<>();

            // TODO: Handle hierarchical keys.
            for (String key : keys) {
                Object value = currentEvent.get(key);
                if (value != null)
                    event.put(key, value);
            }
        }

        return this;
    }

    // Remove the list of keys from event.
    public Parser discard(final List<String> keys) {
        if (topMatcher != null) {
            for (String key : keys) {
                String hierarchicalKeys[] = key.split(HIERARCHICAL_SEPARATOR_SPLIT);
                if (hierarchicalKeys.length == 1) {
                    event.remove(key);
                }
                else {
                    Map<String, Object> tmp = get_last_map_discard(hierarchicalKeys);
                    if (tmp instanceof Map) {
                        tmp.remove(hierarchicalKeys[hierarchicalKeys.length - 1]);
                    }

                }
            }
        }

        return this;
    }

    public Parser insert(final String sourceKey, final int value) {
        return set_value(sourceKey, value);
    }

    public Parser insert(final String sourceKey, final Object value) {
        return set_value(sourceKey, value);
    }

    public Parser insert(final String sourceKey, final Object value, boolean array, String content_type) {
        return set_value(sourceKey, value, array, content_type);
    }

    public Parser copy_raw_log(final String key) {
        set_value(key, log);
        return this;
    }

    public Parser copy(final List<String> fromKeys, final List<String> toKeys) {
        // TODO: Check if fromKeys & toKeys sizes are same.

        for (int i = 0; i < fromKeys.size(); i++) {
            final String fromKey = fromKeys.get(i);
            final String toKey = toKeys.get(i);
            final Object value = get_value(fromKey);
            if (value != null) {
                if (value instanceof String) {
                    String stringValue = (String) value;
                    int length = stringValue.length();
                    StringBuilder buffer = new StringBuilder();
                    for (int j = 0; j < length; j++) {
                        if (stringValue.charAt(j) == '\\') {
                            if (j + 1 < length && stringValue.charAt(j + 1) == '\\')
                                j = j + 1;
                        }
                        buffer.append(stringValue.charAt(j));
                    }
                    set_value(toKey, buffer.toString());
                }
                else
                    set_value(toKey, value);
            } else {
                set_value(toKey, null);
            }
        }

        return this;
    }

    // Every class that inherits Parser must implement the following methods
    public abstract Parser parse();

    public abstract void setInputBuilder(JSONObject input);

}