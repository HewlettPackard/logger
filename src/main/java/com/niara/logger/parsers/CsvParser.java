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


public class CsvParser extends Parser {

    private CsvInputBuilder csvInputBuilder;

    // There should be only 1 instance of InputBuilder for all KeyValueParser object

    public CsvParser(final String log) {
        super(log);
        csvInputBuilder = new CsvInputBuilder();
    }

    public void setInputBuilder(JSONObject input) {
        csvInputBuilder.setParams(input);
    }

    public Parser parse() {
        final char separator = csvInputBuilder.getFieldSeparator();
        final String[] columnNames = csvInputBuilder.getColumnNames();

        String targetKey = csvInputBuilder.getTargetKey();
        final Object tmp = get_value(csvInputBuilder.getSourceKey());
        final String defaultColumnNamePrefix = "column";

        if (tmp != null && tmp instanceof String) {
            String buffer = (String) tmp;

            final int length = buffer.length();
            final int numInputColumns = columnNames.length;
            int startIndex = 0;
            int columnCounter = 1;
            boolean withinQuotes = false;

            // ' or " . Escaping is allowed only within " char.
            char quoteChar = separator;
            for (int i = 0; i < length; i++) {
                final char c = buffer.charAt(i);
                if (withinQuotes) {
                    if (c == '"' && buffer.charAt(i - 1) == '\\') {
                        // It is an escaped char \", so the string is still within quotes.
                    } else if (c == quoteChar) {
                        withinQuotes = false;
                    }
                } else {
                    if (i != 0 && c == '"' && buffer.charAt(i - 1) == '\\') {
                        // It is an escaped char \", so the string is not within quotes.
                    } else if (!csvInputBuilder.shouldIgnoreQuotes() && (c == '"' || c == '\'')) {
                        withinQuotes = true;
                        quoteChar = c;
                    } else if (c == separator) {
                        // End of a value.
                        String key;
                        if (columnCounter <= numInputColumns)
                            key = columnNames[columnCounter - 1];
                        else
                            key = defaultColumnNamePrefix + columnCounter;
                        final String value = get_unquoted_string(buffer, startIndex, i);
                        if(targetKey != null) {
                            set_value(targetKey + " " + key, value);
                        } else {
                            set_value_simple(key, value);
                        }


                        startIndex = i + 1;
                        columnCounter++;
                    }
                }
            }

            // Store last value.
            String key;
            if (columnCounter <= numInputColumns)
                key = columnNames[columnCounter - 1];
            else
                key = defaultColumnNamePrefix + columnCounter;
            final String value = get_unquoted_string(buffer, startIndex, length);
            if(targetKey != null){
                set_value(targetKey + " " + key, value);
            } else {
                set_value_simple(key, value);
            }

        }

        return this;
    }

}