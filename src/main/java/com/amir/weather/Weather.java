package com.amir.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * key类设计
 */
public class Weather implements WritableComparable<Weather> {
    private Integer year;
    private Integer month;
    private Integer day;
    private Double temperature;


    public Weather() {

    }

    /**
     * 该方法可以在排序比较器和分组比较器中调用
     * 但是因为分组比较器和排序比较器规则不同，所以只能满足一方调用
     * 想被谁调用就用对应的逻辑编写
     * @param o the object to be compared.
     * @return
     */
    @Override
    public int compareTo(Weather o) {
        // 安装排序比较器的方式实现
        int result= this.year.compareTo(o.year);
        if (result==0){ // 年相同
            result = this.month.compareTo(o.month);
            if (result==0){ // 月份相同
                result = this.temperature.compareTo(o.temperature);
            }
        }
        return 0;
    }
    /**
     * 通过内部类注册key自带比较器
     */
    public static class Comparator extends org.apache.hadoop.io.WritableComparator {
        protected Comparator() {
            super(Weather.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            // 安装分组比较器的方式实现
            Weather w1 = (Weather) a;
            Weather w2 = (Weather) b;
            // 如果weather类中有排序比较器，则调用compareTo方法，逻辑不同则需要重新实现
            return w1.compareTo(w2);
        }
    }
    static {
        // 注册比较器
        org.apache.hadoop.io.WritableComparator.define(Weather.class, new Comparator());
    }

    /**
     * 序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
        dataOutput.writeInt(day);
        dataOutput.writeDouble(temperature);
    }

    /**
     * 反序列化
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
            this.year=dataInput.readInt();
            this.month=dataInput.readInt();
            this.day=dataInput.readInt();
            this.temperature=dataInput.readDouble();
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    public Integer getDay() {
        return day;
    }

    public void setDay(Integer day) {
        this.day = day;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "Weather{" + "year=" + year + ", month=" + month + ", day=" + day + ", temperature=" + temperature + '}';
    }
}
