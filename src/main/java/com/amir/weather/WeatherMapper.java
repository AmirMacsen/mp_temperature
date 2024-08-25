package com.amir.weather;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 泛型的四个参数类型
 * 1.输入的key的类型： 读取的偏移量
 * 2.输入的value的类型: 读取的文本内容
 * 3.输出的key的类型: 将当前行的数据拆分后，封装成Weather对象
 * 4.输出的value的类型： 输出当前行的数据
 */
public class WeatherMapper extends Mapper<LongWritable, Text, Weather, Text> {
   // 定义输出的key的对象, 在map重定义对象的好处是，对象只用定义一次
    private Weather weather = new Weather();
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Calendar calendar = Calendar.getInstance();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Weather, Text>.Context context) throws IOException, InterruptedException {
        // 数据拆分 2020-10-01 12:21:02   37c
        String line = value.toString().trim();
        // 日期和温度拆开
        String[] fields = line.split("\t");

        // 处理温度： 将温度后面的C去掉
        Double temperature = Double.parseDouble(fields[1].substring(0, fields[1].length() - 1));
        weather.setTemperature(temperature);

        // 处理日期
        try {
            Date date = sdf.parse(fields[0]);
            // 从date中获取年月日
            calendar.setTime(date);
            weather.setYear(calendar.get(Calendar.YEAR));
            // 月份是从0-11的
            weather.setMonth(calendar.get(Calendar.MONTH) + 1);
            weather.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            // 将处理后的数据写入圆形缓冲区
            context.write(weather, value);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
