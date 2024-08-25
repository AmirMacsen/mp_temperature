package com.amir.weather;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reducer
 *
 * 同一个年月会有多条谁，我们只需要温度最高的两天
 *
 * 泛型的四个参数：
 * 1.输入的key的类型： 与mapper输出的key的类型一致
 * 2.输入的value的类型： 与mapper输出的value的类型一致
 * 3.输出的key的类型
 * 4.输出的value的类型
 */
public class WeatherReducer extends Reducer<Weather, Text, Weather, NullWritable> {
    @Override
    protected void reduce(Weather key, Iterable<Text> values, Reducer<Weather, Text, Weather, NullWritable>.Context context) throws IOException, InterruptedException {
        int flag = -1;
        for (Text value : values) {
            // 如果值是 -1 ，说明是第一条，直接输出，因为mapper已经排序了
            if (flag == -1){
                context.write(key, NullWritable.get());
                flag = key.getDay();
            }else{
                if (key.getDay() != flag){ // 下次如果不是同一天的则直接输出
                    context.write(key, NullWritable.get());
                    return;
                }
            }
        }
    }
}
