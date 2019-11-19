package com.base;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestPattern {
    public static void main(String[] args) {
        String input = "123456789";
        List<String> output = new ArrayList<>();

        Pattern pattern = Pattern.compile("^(.{2})(.{2})(.{3})(.{2}).*");
        Matcher matcher = pattern.matcher(input);

        if (matcher.matches()) {
            for (int i = 1; i <= matcher.groupCount(); i++) {
                output.add(matcher.group(i));
            }
        }

        System.out.println(output);
    }
}
