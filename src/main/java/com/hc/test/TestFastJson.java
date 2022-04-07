package com.hc.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class TestFastJson {
    public static void main(String[] args) {
        String str="[\n" +
                "{\n" +
                "\"dps\": {\n" +
                "\"1557936000\": 1893.0,\n" +
                "\"1558022400\": 0.0\n" +
                "}\n" +
                "},\n" +
                "{\n" +
                "\"dps\": {\n" +
                "\"1557936000\": 954.0,\n" +
                "\"1558022400\": 0.0\n" +
                "}\n" +
                "}\n" +
                "]";
        System.out.println(str);
        System.out.println(JSONObject.parseObject(str));
    }
}
