package ru.mephi.hadoop.hw.utils;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

import java.util.LinkedList;
import java.util.List;

import static ru.mephi.hadoop.hw.utils.AppUtils.UNKNOWN_CITY;

public class AppTestUtils {
    public static RandomDataGenerator randomDataGenerator = new RandomDataGenerator();

    public static List<Pair<Text, LongWritable>> expectedMapOutput = new LinkedList<Pair<Text, LongWritable>>() {{
        add(new Pair<>(new Text("kazan"), new LongWritable(2)));
        add(new Pair<>(new Text("moscow"), new LongWritable(1)));
        add(new Pair<>(new Text("ryazan"), new LongWritable(1)));
        add(new Pair<>(new Text("spb"), new LongWritable(2)));
        add(new Pair<>(new Text("tver"), new LongWritable(1)));
        add(new Pair<>(UNKNOWN_CITY, new LongWritable(3)));
        add(new Pair<>(new Text("voronezh"), new LongWritable(1)));
    }};

    public static Text generateInputMapper(String cityId, String biddingPrice) {
        return new Text(
                rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        cityId + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        biddingPrice + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t" +
                        rdmStr() + "\t");
    }

    private static String rdmStr() {
        return RandomStringUtils.randomAlphabetic(randomDataGenerator.nextInt(0, 10));
    }
}
