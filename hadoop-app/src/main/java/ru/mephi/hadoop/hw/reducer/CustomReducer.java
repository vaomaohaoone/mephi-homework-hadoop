package ru.mephi.hadoop.hw.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Кастомный редьюсер класс
 *
 * @author Тимофеев Кирилл
 */
public class CustomReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    /**
     * Поле, служащее результатом на выходе Reduce фазы
     */
    private final LongWritable result = new LongWritable();

    /**
     * Переопределенная функция reduce
     *
     * @param key     - Text ключ
     * @param values  - LongWritable значения
     * @param context - контекст фазы Reduce
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
