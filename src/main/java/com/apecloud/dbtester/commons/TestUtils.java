package com.apecloud.dbtester.commons;

import java.util.Random;

public class TestUtils {
    /**
     * 生成随机字符串
     *
     * @param length 字符串长度
     * @return 随机字符串
     */
    public static String randomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(characters.length());
            sb.append(characters.charAt(index));
        }
        return sb.toString();
    }
}
