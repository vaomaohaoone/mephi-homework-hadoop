package ru.mephi.hadoop.hw;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import ru.mephi.hadoop.hw.mapper.CustomMapper;
import ru.mephi.hadoop.hw.partitioner.CustomPartitioner;
import ru.mephi.hadoop.hw.reducer.CustomReducer;

import java.net.URI;

public class App {

    /**
     * main функция
     *
     * @param args - входные параметры (ind0 - путь до city.en.txt файла, ind1 - путь до папки с входными данными,
     *             ind2 - путь для выходных данных)
     */
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf();
        // выходные данные в виде csv
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "City count by bidding price");
        job.addCacheFile(new URI(args[0]));
        job.setJarByClass(App.class);
        job.setMapperClass(CustomMapper.class);
        job.setCombinerClass(CustomReducer.class);
        job.setReducerClass(CustomReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setPartitionerClass(CustomPartitioner.class);
        // количество редьюсеров - 2 (закомментировать при локальной отладке)
        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        Path out = new Path(args[2]);
        out.getFileSystem(conf).delete(out, true);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

