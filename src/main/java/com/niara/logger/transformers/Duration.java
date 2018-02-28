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
import org.json.simple.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Duration extends Transformer {

    private String key;
    private String format;
    private final static String BASE_TIME = "00:00:00";
    private final static SimpleDateFormat baseFormat = new SimpleDateFormat("HH:mm:ss");

    @Override
    public void setParams(String key, JSONObject params) {
        this.key = key;
        this.format = (String) params.get("format");
        if (this.format == null)
            this.format = "HH:mm:ss";
    }

    @Override
    public Parser transform(Parser parser) {
        // Converts an ISOFormat time string to seconds
        final Object value = parser.get_value(key);
        if (value != null && value instanceof String) {
            final SimpleDateFormat dateFormat = new SimpleDateFormat(format);
            try {
                final Date baseDate = baseFormat.parse(BASE_TIME);
                final Date date = dateFormat.parse((String) value);
                long seconds = (date.getTime() - baseDate.getTime()) / 1000L;
                parser.set_value(key, seconds);
            } catch (ParseException e) {
                logger.error("Cannot parse {} key, specified in format: {}", key, format);
                logger.error("DurationTransformer TimeParsingException", e);
            }
        }
        return parser;
    }

}
