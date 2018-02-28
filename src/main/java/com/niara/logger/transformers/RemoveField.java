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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class RemoveField extends Transformer {

    private String key;
    private String condition;
    private List<String> values;
    private boolean isRegex = false;

    @Override
    public void setParams(String key, JSONObject params) {
        this.key = key;
        this.values = (List<String>)params.get("values");
        this.condition = (String) params.get("condition");
        if (this.condition == null)
            this.condition = "equals";
        if (params.get("regex") != null)
            this.isRegex = (boolean) params.get("regex");
    }

    @Override
    public Parser transform(Parser parser) {
        final Object tmp = parser.get_value(key);
        if (tmp != null && tmp instanceof String) {
            for (String value: values) {
                boolean canRemove = false;
                if (condition.equals("equals")) {
                    if (isRegex) {
                        Pattern pattern = Pattern.compile(value);
                        Matcher matcher = pattern.matcher((String) tmp);
                        canRemove = matcher.matches();
                    } else {
                        canRemove = value.equals(tmp);
                    }
                } else if (condition.equals("not equals")) {
                    if (isRegex) {
                        Pattern pattern = Pattern.compile(value);
                        Matcher matcher = pattern.matcher((String) tmp);
                        canRemove = !matcher.matches();
                    } else {
                        canRemove = !value.equals(tmp);
                    }
                }

                if (canRemove) {
                    parser.remove_key(key);
                    return parser;
                }
            }
        }
        return parser;
    }

}
