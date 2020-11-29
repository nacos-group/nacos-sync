package com.alibaba.nacossync.constant;

/**
 * Created by maj on 2020/11/18.
 */
public enum ShardingLogTypeEnum {

    ADD("add", "新增"),

    DELETE("DELETE", "删除");

    private String type;
    private String desc;

    ShardingLogTypeEnum(String type, String desc) {
        this.type = type;
        this.desc = desc;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public static boolean contains(String type) {

        for (ShardingLogTypeEnum shardingLogTypeEnum : ShardingLogTypeEnum.values()) {

            if (shardingLogTypeEnum.getType().equals(type)) {
                return true;
            }
        }
        return false;
    }
}
