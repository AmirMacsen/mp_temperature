package com.amir.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherSortComparator extends WritableComparator {
    public WeatherSortComparator() {
        // 让当前类的父类创建一个weather对象
        super(Weather.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 强制类型转换
        Weather w1 = (Weather) a;
        Weather w2 = (Weather) b;
        // 如果weather类中compareTo逻辑与排序比较器相同，则调用compareTo方法，逻辑不同则需要重新实现
        int result = w1.getYear().compareTo(w2.getYear());
        if (result == 0) {
            result = w1.getMonth().compareTo(w2.getMonth());
            if (result == 0) {
                result = w1.getTemperature().compareTo(w2.getTemperature());
            }
        }
        return result;
    }
}
