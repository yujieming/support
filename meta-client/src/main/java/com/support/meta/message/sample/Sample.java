package com.support.meta.message.sample;

import com.support.meta.message.Message;
import com.support.meta.transport.MetaType;

public class Sample implements Message {

    public static final Sample TEMPLE = new Sample();
    private static final String SAMPLE_META_TYPE = "sample";


    private String name;
    private Integer age;
    private String clazz;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    @Override
    public MetaType getType() {
        return () -> SAMPLE_META_TYPE;
    }
}
