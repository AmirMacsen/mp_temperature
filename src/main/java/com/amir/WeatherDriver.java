package com.amir;

import com.amir.weather.Weather;
import com.amir.weather.WeatherGroupingComparator;
import com.amir.weather.WeatherMapper;
import com.amir.weather.WeatherPartitioner;
import com.amir.weather.WeatherReducer;
import com.amir.weather.WeatherSortComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherDriver {
    public static void main(String[] args) throws Exception {
        //1.判断输入输出路径是否合法
        if(args==null||args.length!=2){
            System.out.println("Usage:yarn jar myweather.jar com.itbaizhan.WeatherDriver <inputPath> <outputPath>");
            System.exit(1);
        }
        //2.创建配置文件对象，并加载默认的配置
        Configuration configuration = new Configuration(true);
        //3.设置本地运行
        configuration.set("mapreduce.framework.name","local");
        configuration.set("mapreduce.cluster.local.dir","/Users/maningyu/workspace/javaprojects/hadoop_temperature/src/main/resources");
        //4.创建Job对象
        Job job = Job.getInstance(configuration);
        //5.设置job对象的相关参数
        //5.1设置入口类
        job.setJarByClass(WeatherDriver.class);
        //5.2设置Mapper类以及Mapper输出的key和value的类型
        job.setMapperClass(WeatherMapper.class);
        job.setMapOutputKeyClass(Weather.class);
        job.setMapOutputValueClass(Text.class);
        //5.3设置Reduce类以及Reducer输出的key和value的类型
        job.setReducerClass(WeatherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //6.设置输入路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        //7.设置输出路径
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(outputPath)){
            //如果存在，则将之前的目录删除掉
            fileSystem.delete(outputPath,true);
        }
        FileOutputFormat.setOutputPath(job,outputPath);
        //8.设置Reduce任务数量和分区类
        job.setNumReduceTasks(4);
        job.setPartitionerClass(WeatherPartitioner.class);
        //9.设置排序比较器
        job.setSortComparatorClass(WeatherSortComparator.class);
        //10.设置分组比较器
        job.setGroupingComparatorClass(WeatherGroupingComparator.class);
        //11.提交作业
        job.waitForCompletion(true);
    }
}
