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
import org.json.simple.JSONObject;

import java.util.Date;


public class DateToEpoch extends Transformer {

    private String key;

    @Override
    public void setParams(String key, JSONObject params) {
        this.key = key;
    }

    @Override
    public Parser transform(Parser parser) {
        final Object tmp = parser.get_value(key);
        if (tmp != null) {
            if( tmp instanceof Date) {
                final Date date = (Date) tmp;
                parser.set_value(key, date.getTime() * 1000);
            } else if (tmp instanceof DateTime) {
                final DateTime dateTime = (DateTime) tmp;
                parser.set_value(key, dateTime.getMillis() * 1000);
            }
        }

        return parser;
    }
}
