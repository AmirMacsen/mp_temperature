package com.amir.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区比较器，需要保证把逻辑上相同的数据分到一个分区里
 * 而且要注意负载均衡，防止数据倾斜
 * 典型的分区方法有：
 * 按月分： 【10月，11月，12月】【1月，2月，3月】【4月，5月，6月】【7月，8月，9月】 在足够了解数据的情况下这么分也可以
 * 按月分： 根据月份对分区数比如说4取余
 * 我们采用后一种分发
 *
 * 泛型中的key和value分别代表mapper输出的key和value类型
 */
public class WeatherPartitioner extends Partitioner<Weather, Text> {
    /**
     * 获取分区号
     * @param weather weather对象
     * @param text text对象
     * @param i 分区数
     * @return
     */
    @Override
    public int getPartition(Weather weather, Text text, int i) {
        return weather.getMonth()%i;
    }
}
