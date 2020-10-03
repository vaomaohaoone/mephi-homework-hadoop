package ru.mephi.hadoop.hw.utils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.List;

public class AppUtils {
    public static final LongWritable ONE = new LongWritable(1);
    public static final Text UNKNOWN_CITY = new Text("unknown");
    public static final String CITY_FILE_NAME = "city.en.txt";
    public static final List<String> EMPTY_PLACEHOLDERS = Arrays.asList("null", "Na");
    public static final String SEPARATE_SYMBOL = "\t";
    public static final int BIDDING_PRICE_INDEX = 19;
    public static final int CITY_ID_INDEX = 7;
    public static final int BIDDING_PRICE_THRESHOLD = 250;
}
