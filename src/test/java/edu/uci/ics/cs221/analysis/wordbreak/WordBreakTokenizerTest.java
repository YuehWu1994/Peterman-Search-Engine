package edu.uci.ics.cs221.analysis.wordbreak;

import edu.uci.ics.cs221.analysis.WordBreakTokenizer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class WordBreakTokenizerTest {

    @Test
    public void test1() {
        String text = "catdog";
        /*
        test below string and test "I want to have peanut butter sandwich" <<
        return peanut, butter, sandwich, not pea, nut, butter, sandwich
        test unbreakable text
         */
        List<String> expected = Arrays.asList("cat", "dog");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));

    }
    @Test
    public void test2() {
        String text = "catanddog";
        List<String> expected = Arrays.asList("cat", "dog");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));

    }
    @Test
    public void test3()
    {
        String text = "IWANTtohavepeanutbuttersandwich";
        List<String> expected = Arrays.asList("want", "peanut", "butter", "sandwich");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }
    @Test
    public void test4() {
        String text = "Itisnotourgoal";
        // Original: "It is not our goal"
        // we didn't use "It's" because "it's" is not in the provided dictionary.
        // It's easy to be broken into "it is no tour goal", which is false.

        List<String> expected = Arrays.asList("goal");
        // "it" "is" "not" "our" are all stop words, which should be discarded.
        // False: {"tour", "goal"}

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void test5() {
        String text = "FindthelongestpalindromicstringYoumayassumethatthemaximumlengthisonehundred";
        // Original: "Find the longest palindromic string. You may assume that the maximum length is one hundred."
        // Test if the WordBreaker is efficient enough to handle long complex strings correctly.

        List<String> expected = Arrays.asList("find", "longest", "palindromic", "string", "may",
                "assume", "maximum", "length", "one", "hundred");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }
    @Test
    public void test6() {
        String text = "THISiswhATItoldyourI'llFRIendandI'llgoonlinecontactcan'tforget";
        List<String> expected = Arrays.asList("old", "i'll", "friend", "i'll","go","online","contact","can't","forget");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));

    }

    //check whether the work-break functions when meet strings like "whatevergreen" and the string constists of more than one frequenctly used word
    @Test
    public void test7(){
        String text = "informationinforTHOUGHTFULLYcopyrightwhatevercontactablewhatevergreen";
        List<String> expected = Arrays.asList("information", "thoughtfully", "copyright", "whatever", "contact", "able","whatever", "green" );

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }


    //check if the program can throw an exception when the string is unbreakable
    @Test(expected = RuntimeException.class)
    public void test8(){
        String text = "$reLLL(  ghn)iog*";
        //throw exception
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);

    }
    @Test
    public void test9() {
        String text = "IlOveSAnFrancIsCo";
        List<String> expected = Arrays.asList("love", "san", "francisco");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));

    }

    /*
     * The test2() has only the stopwords.
     * Hence the output must be an empty array and should not throw any exception as the string can broken into
     * words that exist in the dictionary.
     * Additionally, this testcase also tests the program to handle a really long string.
     * */

    @Test
    public void test10() {
        String text = "imemymyselfweouroursourselvesyouyouryoursyourselfyourselveshehimhishimselfsheherhersherselfititsitselftheythemtheirtheirsthemselveswhatwhichwhowhomthisthatthesethoseamisarewaswerebebeenbeinghavehashadhavingdodoesdiddoing";
        List<String> expected = Arrays.asList();
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));

    }

    /*
     * The test3() has an empty string
     * Hence the wordbreak tokenizer must return empty set of tokens
     * */

    @Test
    public void test11() {
        String text = "";
        List<String> expected = Arrays.asList();
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }


    /*
     * The test4() has string with punctuations
     * Hence the wordbreak tokenizer must throw an exception
     * */

    @Test(expected = RuntimeException.class)
    public void test12() {
        String text = "mother-in-law";
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);
    }

    /*
     * The test5() has two possible set of tokens
     * But the wordbreak tokenizer must come up with the most optimal one that is ["hello", "range"] instead of ["hell", "orange"]
     * */

    @Test
    public void test13() {
        String text = "hellorange";
        List<String> expected = Arrays.asList("hello","range");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text)); //must expect an exception

    }
    @Test
    public void test14() {
        String text = "thereareelevenpineapples";
        List<String> expected = Arrays.asList("eleven", "pineapples");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test(expected = RuntimeException.class)
    public void test15() {
        String text = "fralprtnqela";
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);

    }
    /*
     In the second test we want to test the ability to find matches
     that are lower cased and the ability to remove the stop words.
     In order to do this we are creating a sentence that
     */
    @Test
    public void test16() {
        String text = "WEhaveaCOOLTaskinFrontOfUSANDwEShouldbehavingAgoodTIme";
        List<String> expected = Arrays.asList("cool","task","front","us","behaving","good","time");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));

    }

    @Test(expected = RuntimeException.class)
    public void test17() {
        String text = "WhatHappensWhenWeaddAperiod.";
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);

    }

    @Test(expected = RuntimeException.class)
    public void test18() {
        String text = "This is too check if an exception is thrown when there are spaces";
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);

    }

    @Test
    public void test19()
    {
        String text = "ThiscourseexposesstudentstoprincipalconceptsrelatedtoinformationretrievalincludingtextstemmingandtextindexinginvertedindexsearchbooleanexpressionsphrasesearchandrankingItwillalsocovertopicsofWebsearchandverticalsearchwithrelatedtechniquessuchascrawlingandsearchengineoptimizationAsignificantpartofthecourseistoimplementasearchengineusingJava";
        List<String> expected = Arrays.asList("course","exposes","students","principal","concepts",
                "related", "information","retrieval","including","text", "stemming", "text",
                "indexing", "inverted", "index", "search", "boolean", "expressions", "phrase",
                "search", "ranking", "also", "cover", "topics", "web", "search", "vertical", "search",
                "related", "techniques", "crawling", "search", "engine", "optimization", "significant",
                "part", "course", "implement", "search", "engine", "using", "java");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }
}
