package ru.mephi.hadoop.hw;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import ru.mephi.hadoop.hw.mapper.CustomMapper;
import ru.mephi.hadoop.hw.reducer.CustomReducer;
import ru.mephi.hadoop.hw.utils.AppTestUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static ru.mephi.hadoop.hw.utils.AppUtils.*;
import static ru.mephi.hadoop.hw.utils.AppTestUtils.expectedMapOutput;

public class AppTest {
    private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    private ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
    private static final String POSITIVE_BIDDING_PRICE = String.valueOf(BIDDING_PRICE_THRESHOLD + 1);
    private static final String NEGATIVE_BIDDING_PRICE = String.valueOf(BIDDING_PRICE_THRESHOLD);

    @Before
    public void before() {
        CustomMapper customMapper = new CustomMapper();
        CustomReducer customReducer = new CustomReducer();
        mapDriver = MapDriver.newMapDriver(customMapper).withCacheFile(CITY_FILE_NAME);
        reduceDriver = ReduceDriver.newReduceDriver(customReducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(customMapper, customReducer).withCacheFile(CITY_FILE_NAME);
    }

    @Test
    public void testMapper() throws IOException {
        List<Pair<Text, LongWritable>> result = mapDriver
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("0", NEGATIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("1", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("2", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("3", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("4", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("5", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("0", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("1", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("2", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("3", NEGATIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("Na", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("1000", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("null", NEGATIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("null", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("333", NEGATIVE_BIDDING_PRICE))
                .run();
        assertEquals(11, result.size());
        assertEquals(expectedMapOutput.stream().map(Pair::getFirst).collect(Collectors.toSet()),
                result.stream().map(Pair::getFirst).collect(Collectors.toSet()));
        assertEquals(3L, result.stream().filter(pair -> pair.getFirst().equals(UNKNOWN_CITY)).count());
        for (Pair<Text, LongWritable> item : result) {
            assertEquals(new LongWritable(1), item.getSecond());
        }
    }

    @Test
    public void testReducer() throws IOException {
        List<Pair<Text, LongWritable>> result = reduceDriver
                .withInput(new Text("moscow"), new LinkedList<LongWritable>() {{
                    add(new LongWritable(1));
                }})
                .withInput(new Text("kazan"), new LinkedList<LongWritable>() {{
                    add(new LongWritable(1));
                    add(new LongWritable(1));
                }})
                .withInput(new Text("spb"), new LinkedList<LongWritable>() {{
                    add(new LongWritable(1));
                    add(new LongWritable(1));
                }})
                .withInput(new Text("ryazan"), new LinkedList<LongWritable>() {{
                    add(new LongWritable(1));
                }})
                .withInput(new Text("tver"), new LinkedList<LongWritable>() {{
                    add(new LongWritable(1));
                }})
                .withInput(new Text("voronezh"), new LinkedList<LongWritable>() {{
                    add(new LongWritable(1));
                }})
                .withInput(UNKNOWN_CITY, new LinkedList<LongWritable>() {{
                    add(new LongWritable(1));
                    add(new LongWritable(1));
                    add(new LongWritable(1));
                }})
                .run();
        assertEquals(expectedMapOutput.size(), result.size());
        assertEquals(1L, result.stream().filter(textLongWritablePair -> textLongWritablePair.getSecond().get() == 3).count());
        assertEquals(2L, result.stream().filter(textLongWritablePair -> textLongWritablePair.getSecond().get() == 2).count());
        assertEquals(4L, result.stream().filter(textLongWritablePair -> textLongWritablePair.getSecond().get() == 1).count());
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("0", NEGATIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("1", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("2", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("3", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("4", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("5", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("0", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("1", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("2", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("3", NEGATIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("Na", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("1000", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("null", NEGATIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("null", POSITIVE_BIDDING_PRICE))
                .withInput(new LongWritable(), AppTestUtils.generateInputMapper("333", NEGATIVE_BIDDING_PRICE))
                .withAllOutput(expectedMapOutput)
                .runTest();
    }
}
