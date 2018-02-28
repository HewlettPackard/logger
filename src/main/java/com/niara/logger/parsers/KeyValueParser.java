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

import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class KeyValueParser extends Parser {

    private KeyValueInputBuilder keyValueInputBuilder;

    public KeyValueParser(final String log) {
        super(log);
        keyValueInputBuilder = new KeyValueInputBuilder();
    }

    public void setInputBuilder(JSONObject input) {
        keyValueInputBuilder.setParams(input);
    }

    @Override
    public Parser parse() {
        final String sourceKey = keyValueInputBuilder.getSourceKey();
        final String targetKey = keyValueInputBuilder.getTargetKey();
        final String keyPrefix = keyValueInputBuilder.getKeyPrefix();
        final String fieldSeparator = keyValueInputBuilder.getFieldSeparator();
        final String valueSeparator = keyValueInputBuilder.getValueSeparator();

        final Object tmp = get_value(sourceKey);
        if (tmp != null && tmp instanceof String) {
            final String value = (String) tmp;
            final Map<String, Object> map = new HashMap<>();

            int startIndex = 0;
            int fieldIndex;
            while ((fieldIndex = value.indexOf(fieldSeparator, startIndex)) != -1) {
                if (value.indexOf(valueSeparator, startIndex) != -1) {
                    int valueSeparatorIndex = value.indexOf(valueSeparator, startIndex);
                    char firstChar = value.charAt(valueSeparatorIndex + 1);
                    if (firstChar == '"' || firstChar == '\'') {
                        fieldIndex = value.indexOf(firstChar, valueSeparatorIndex + 2) + 1;
                    }
                }
                add_kv(value, valueSeparator, startIndex, fieldIndex, map, keyPrefix);
                startIndex = fieldIndex + fieldSeparator.length();
            }

            final int length = value.length();

            if (startIndex < length)
                add_kv(value, valueSeparator, startIndex, length, map, keyPrefix);

            // If any key/value pair found, then only set the value.
            if (!map.isEmpty()) {
                if (targetKey!= null) {
                    final String[] keys = targetKey.split(HIERARCHICAL_SEPARATOR_SPLIT);
                    final Map<String, Object> lastMap = get_last_map(keys);
                    final String lastKey = keys[keys.length - 1];
                    final Object current = lastMap.get(lastKey);
                    if (current == null) {
                        lastMap.put(lastKey, map);
                    } else if (current instanceof Map) {
                        map.putAll((Map<String, Object>) current);
                        lastMap.put(lastKey, map);
                    }
                } else {
                    event.putAll(map);
                }

            }
        }

        return this;
    }

}
