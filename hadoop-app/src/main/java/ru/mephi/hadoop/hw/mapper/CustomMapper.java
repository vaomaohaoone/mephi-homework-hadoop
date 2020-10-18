package ru.mephi.hadoop.hw.mapper;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static ru.mephi.hadoop.hw.utils.AppUtils.*;

/**
 * Кастомный маппер класс
 *
 * @author Тимофеев Кирилл
 */
public class CustomMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    /**
     * Поле, служащее ключом на выходе Map фазы
     */
    private static final Text city = new Text();
    /**
     * Значения для map-side-join
     */
    private static final HashMap<String, String> cityMap = new HashMap<>();

    /**
     * Переопределенная функция предустановки.
     * Вызывается один раз перед началом задачи
     *
     * @param context - контекст маппера {@link org.apache.hadoop.mapreduce.Mapper.Context}
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        List<URI> files = Arrays.asList(context.getCacheFiles());
        Optional<URI> cityFileOptional = files.stream().filter(uri -> uri.getPath().contains(CITY_FILE_NAME)).findFirst();
        if (!cityFileOptional.isPresent())
            throw new IOException();
        Path path = new Path(cityFileOptional.get());
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = br.readLine()) != null) {
            String[] splits = line.split(SEPARATE_SYMBOL);
            cityMap.put(splits[0], splits[1]);
        }
        br.close();
        in.close();
    }

    /**
     * Переопределенная функция отображения. Вызывается после setup
     *
     * @param key     - входной ключ
     * @param value   - входное значение
     * @param context - контекст маппера {@link org.apache.hadoop.mapreduce.Mapper.Context}
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split(SEPARATE_SYMBOL);
        if (!EMPTY_PLACEHOLDERS.contains(splits[BIDDING_PRICE_INDEX])) {
            int biddingPrice = Integer.parseInt(splits[BIDDING_PRICE_INDEX]);
            if (biddingPrice > BIDDING_PRICE_THRESHOLD) {
                if (cityMap.containsKey(splits[CITY_ID_INDEX])) {
                    city.set(cityMap.get(splits[CITY_ID_INDEX]));
                } else {
                    city.set(UNKNOWN_CITY);
                }
                context.write(city, ONE);
            }
        }
    }
}
