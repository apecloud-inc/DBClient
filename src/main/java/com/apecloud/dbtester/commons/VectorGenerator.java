package com.apecloud.dbtester.commons;

import java.util.Random;
import java.util.stream.DoubleStream;

public class VectorGenerator {

    /**
     * 生成指定长度的随机向量（默认范围 [0.0, 1.0)）
     * @param length 向量长度
     * @return 随机向量数组
     */
    public static double[] generateRandomVector(int length) {
        return generateRandomVector(length, 0.0, 1.0);
    }

    /**
     * 生成指定长度和范围的随机向量
     * @param length 向量长度
     * @param min 随机值下限（包含）
     * @param max 随机值上限（不包含）
     * @return 随机向量数组
     * @throws IllegalArgumentException 如果长度无效或范围错误
     */
    public static double[] generateRandomVector(int length, double min, double max) {
        // 验证参数有效性
        if (length <= 0) {
            throw new IllegalArgumentException("向量长度必须为正整数");
        }
        if (Double.isNaN(min) || Double.isNaN(max)) {
            throw new IllegalArgumentException("范围参数不能为NaN");
        }
        if (min >= max) {
            throw new IllegalArgumentException("最大值必须大于最小值");
        }
        if (Double.isInfinite(min) || Double.isInfinite(max)) {
            throw new IllegalArgumentException("范围参数不能为无限值");
        }

        Random random = new Random();
        return DoubleStream.generate(() -> min + (max - min) * random.nextDouble())
                .limit(length)
                .toArray();
    }

    public static void main(String[] args) {
        // 示例用法
        double[] defaultVector = generateRandomVector(3);
        System.out.println("默认范围向量: " + java.util.Arrays.toString(defaultVector));

        double[] customVector = generateRandomVector(5, -10.0, 10.0);
        System.out.println("自定义范围向量: " + java.util.Arrays.toString(customVector));
    }
}