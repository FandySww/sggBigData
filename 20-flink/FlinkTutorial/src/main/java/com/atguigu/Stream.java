package com.atguigu;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Stream {
    public static void main(String[] args) {
        String[] words = new String[]{"Hello","World"};
        List s1 = Arrays.asList(words).stream().map(str -> str.split("")).
                        flatMap(str -> Arrays.stream(str)).
                         collect(Collectors.toList());
        System.out.println(s1);
    }
}
