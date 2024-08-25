package com.amir.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组比较器的实现
 * 根据年月分组，相同的放在同一个分组中
 */
public class WeatherGroupingComparator extends WritableComparator {
    public WeatherGroupingComparator() {
        // 让当前类的父类创weather对象，否则会出现空指针异常
        super(Weather.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 强制类型转换
        Weather w1 = (Weather) a;
        Weather w2 = (Weather) b;
        // weather类中的逻辑不符合分组规则，所以需要自己编写分组逻辑
        int result = w1.getYear().compareTo(w2.getYear());
        if (result == 0) {
            result =  w1.getMonth().compareTo(w2.getMonth());
        }

       return result;
    }
}
