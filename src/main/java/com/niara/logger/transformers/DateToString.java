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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;


public class DateToString extends Transformer {

    private String key;
    private String format;
    private String timeZone;

    @Override
    public void setParams(String key, JSONObject params) {
        this.key = key;
        this.format = (String) params.get("format");
        this.timeZone = (String) params.get("timezone");
    }

    @Override
    public Parser transform(Parser parser) {
        // Converts a Date/DateTime object to a new Date/DateTime object with optional timezone
        final Object tmp = parser.get_value(key);
        if (tmp != null) {
            String date = null;
            if (tmp instanceof Date) {
                final Date value = (Date) tmp;
                final SimpleDateFormat dateFormat = new SimpleDateFormat(format);
                if (timeZone != null)
                    dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
                date = dateFormat.format(value);
                parser.set_value(key, date);
            } else if (tmp instanceof DateTime) {
                final DateTime dt = (DateTime) tmp;
                DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(format);
                if (timeZone != null)
                    dateTimeFormatter = dateTimeFormatter.withZone(DateTimeZone.forID(timeZone));
                date = dateTimeFormatter.print(dt);
            }
            parser.set_value(key, date);
        }

        return parser;
    }

}
