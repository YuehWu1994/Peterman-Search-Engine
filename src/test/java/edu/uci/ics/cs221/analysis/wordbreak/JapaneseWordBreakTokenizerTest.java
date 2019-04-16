package edu.uci.ics.cs221.analysis.wordbreak;

import edu.uci.ics.cs221.analysis.JapaneseWordBreakTokenizer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JapaneseWordBreakTokenizerTest {

    @Test
    public void test1()
    {
        String text = "蓼食う虫も好き好き";
        List<String> expected = Arrays.asList("蓼", "食う", "虫", "も", "好き", "好き");
        JapaneseWordBreakTokenizer tokenizer = new JapaneseWordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void test2()
    {
        String text = "猿も木から落ちる";
        List<String> expected = Arrays.asList("猿", "も", "木", "から", "落ちる");//here the
        // last token can be split into three characters but their probability would be much lower
        JapaneseWordBreakTokenizer tokenizer = new JapaneseWordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void test3()
    {
        String text = "虎穴に入らずんば虎子を得ず";
        List<String> expected = Arrays.asList("虎", "穴", "に", "入ら", "ず", "ん", "ば", "虎", "子", "を", "得", "ず");
        JapaneseWordBreakTokenizer tokenizer = new JapaneseWordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void test4()
    {
        String text = "二兎を追う者は一兎をも得ず";
        List<String> expected = Arrays.asList("二", "兎", "を", "追う", "者", "は", "一", "兎", "を", "も", "得", "ず");//here the
        // last token can be split into three characters but their probability would be much lower
        JapaneseWordBreakTokenizer tokenizer = new JapaneseWordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void test5()
    {
        String text = "門前の小僧習わぬ経を読む";
        List<String> expected = Arrays.asList("門前", "の", "小僧", "習わ", "ぬ", "経", "を", "読む");
        JapaneseWordBreakTokenizer tokenizer = new JapaneseWordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }
}
