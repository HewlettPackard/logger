package com.niara.logger.transformers;

import com.niara.logger.parsers.Parser;
import org.json.simple.JSONObject;


public class ToFloat extends Transformer {

    private String key;

    @Override
    public void setParams(String key, JSONObject params) {
        this.key = key;
    }

    @Override
    public Parser transform(Parser parser) {
        if (parser.get_value(key) == null) {
            logger.debug("ToFloat transformer failed to transform. Value for Key {} is null", key);
            return parser;
        }
        if (parser.get_value(key) instanceof Double) {
            Double doubleValue = Double.parseDouble(parser.get_value(key).toString());
            return parser.set_value(key, doubleValue.floatValue());
        }
        return parser.set_value(key, Float.parseFloat(parser.get_value(key).toString()));
    }

}
