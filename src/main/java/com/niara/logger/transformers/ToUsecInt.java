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


public class ToUsecInt extends Transformer {

    private String key;
    private static long MICROSECONDS_MULTIPLIER = 1000000L;
    private static int MICROSECONDS_LENGTH = 16;
    private static int SECONDS_LENGTH = 10;
    private static long MIN_LENGTH = 1L;

    @Override
    public void setParams(String key, JSONObject params) {
        this.key = key;
    }

    @Override
    public Parser transform(Parser parser) {
        if (parser.get_value(key) == null) {
            logger.debug("ToUsecInt transformer failed to transform. Value for Key {} is null", key);
            return parser;
        }

        long multiplier;
        String timestamp = parser.get_value(key).toString();

        if (timestamp.length() <= SECONDS_LENGTH) {
            multiplier = MICROSECONDS_MULTIPLIER;
        } else {
            int difference = MICROSECONDS_LENGTH - timestamp.length();
            if (difference <= 0) {
                multiplier = MIN_LENGTH;
            } else {
                multiplier = new Double(Math.pow(SECONDS_LENGTH, difference)).longValue();
            }
        }

        return parser.set_value(key, Long.parseLong(timestamp) * multiplier);
    }

}
