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

package com.niara.logger.transformers;

import com.niara.logger.parsers.Parser;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;


public class StringToDate extends Transformer {

    private String key;
    private List<String> formats;
    private String timeZone;

    @Override
    public void setParams(String key, JSONObject params) {
        this.key = key;
        this.formats = (JSONArray) params.get("formats");
        this.timeZone = (String) params.get("timezone");
    }

    @Override
    public Parser transform(Parser parser) {
        // Converts an ISOFormat datetime string to Date/DateTime object
        for (String format : formats) {
            try {
                String newFormat = format;
                final Object tmp = parser.get_value(key);
                if (tmp != null && tmp instanceof String) {
                    String value = (String) tmp;
                    if (format.contains(".SSSSS")) {
                        String parts[] = value.split(".[0-9]{6}");
                        if (parts.length > 0) {
                            String timestampWithoutMicroSeconds = "";
                            for (int i = 0; i < parts.length; i++) {
                                timestampWithoutMicroSeconds += parts[i];
                            }
                            value = timestampWithoutMicroSeconds;
                            newFormat = format.replace(".SSSSSS", "");
                        }
                    }
                    final SimpleDateFormat dateFormat = new SimpleDateFormat(newFormat);
                    if (timeZone != null) {
                        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
                    }
                    try {
                        final Date date = dateFormat.parse(value);
                        parser.set_value(key, date);
                    } catch (ParseException e) {
                        // Try using joda StringToDate
                        logger.debug("Failed to parse date string: {}, format: {}, {}", value, newFormat, e.getMessage());
                        DateTimeFormatter fmt = org.joda.time.format.DateTimeFormat.forPattern(newFormat);
                        parser.set_value(key, fmt.parseDateTime(value));
                    }
                }
            } catch (Exception e) {
                logger.debug("DateTime parsing failed with format: {}", format);
                continue;
            }
            break;
        }
        return parser;
    }

}
