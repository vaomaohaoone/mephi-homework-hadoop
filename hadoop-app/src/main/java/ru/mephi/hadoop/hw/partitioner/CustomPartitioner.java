package ru.mephi.hadoop.hw.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import static ru.mephi.hadoop.hw.utils.AppUtils.UNKNOWN_CITY;

/**
 * Кастомный Partitioner класс
 *
 * @author Тимофеев Кирилл
 */
public class CustomPartitioner extends Partitioner<Text, LongWritable> {
    /**
     * Переопределенный метод выбора партиции
     *
     * @param key            - ключ Text
     * @param value          - значение LongWritable
     * @param numReduceTasks - количество редьюсеров
     * @return возвращает номер партиции (если city unknown, возвращаем 0, иначе 1)
     */
    @Override
    public int getPartition(Text key, LongWritable value, int numReduceTasks) {
        if (numReduceTasks == 0) {
            return 0;
        } else if (key.equals(UNKNOWN_CITY)) {
            return 0;
        } else {
            return 1;
        }
    }
}
