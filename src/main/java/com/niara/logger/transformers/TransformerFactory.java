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

public class TransformerFactory {

    public static Transformer createTransformer(String transformerType) {
        Transformer transformer = null;

        switch(transformerType) {
            case "to_long":
                transformer = new ToLong();
                break;
            case "to_string":
                transformer = new ToString();
                break;
            case "to_lower_case":
                transformer = new LowerCaseValue();
                break;
            case "to_upper_case":
                transformer = new UpperCaseValue();
                break;
            case "replace":
                transformer = new Replace();
                break;
            case "split_and_take":
                transformer = new SplitAndTake();
                break;
            case "string_to_date":
                transformer = new StringToDate();
                break;
            case "date_to_string":
                transformer = new DateToString();
                break;
            case "date_to_epoch":
                transformer = new DateToEpoch();
                break;
            case "to_usec_int":
                transformer = new ToUsecInt();
                break;
            case "trim":
                transformer = new Trim();
                break;
            case "map_value":
                transformer = new MapValue();
                break;
            case "raw_log":
                transformer = new CopyRawLog();
                break;
            case "remove_field":
                transformer = new RemoveField();
                break;
            case "current_epoch_time":
                transformer = new CurrentEpochTime();
                break;
            case "duration":
                transformer = new Duration();
                break;
        }

        return transformer;
    }

    public static Transformer getTransformer(String transformerType) throws Exception {
        if (transformerType == null) {
            throw new Exception("Missing transformer type");
        }

        Transformer transformer = createTransformer(transformerType);

        if (transformer == null) {
            throw new Exception("Unsupported transformer type: " + transformerType);
        }

        return transformer;
    }

}
