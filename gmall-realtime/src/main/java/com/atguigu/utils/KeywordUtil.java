package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    public static List<String> splitKeyWord(String keyword) {

        //创建集合用于存放最终数据
        ArrayList<String> result = new ArrayList<>();

        StringReader reader = new StringReader(keyword);

        //创建IK分词对象
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        try {

            Lexeme next = ikSegmenter.next();

            while (next != null) {
                //取出切分好的词
                result.add(next.getLexemeText());
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //返回结果
        return result;
    }

    public static void main(String[] args) {

        System.out.println(splitKeyWord("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待"));

    }

}
