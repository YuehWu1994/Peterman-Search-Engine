package edu.uci.ics.cs221.analysis;


import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


/**
 * Project 1, task 2: Implement a Dynamic-Programming based Word-Break Tokenizer.
 * <p>
 * Word-break is a problem where given a dictionary and a string (text with all white spaces removed),
 * determine how to break the string into sequence of words.
 * For example:
 * input string "catanddog" is broken to tokens ["cat", "and", "dog"]
 * <p>
 * We provide an English dictionary corpus with frequency information in "resources/cs221_frequency_dictionary_en.txt".
 * Use frequency statistics to choose the optimal way when there are many alternatives to break a string.
 * For example,
 * input string is "ai",
 * dictionary and probability is: "a": 0.1, "i": 0.1, and "ai": "0.05".
 * <p>
 * Alternative 1: ["a", "i"], with probability p("a") * p("i") = 0.01
 * Alternative 2: ["ai"], with probability p("ai") = 0.05
 * Finally, ["ai"] is chosen as result because it has higher probability.
 * <p>
 * Requirements:
 * - Use Dynamic Programming for efficiency purposes.
 * - Use the the given dictionary corpus and frequency statistics to determine optimal alternative.
 * The probability is calculated as the product of each token's probability, assuming the tokens are independent.
 * - A match in dictionary is case insensitive. Output tokens should all be in lower case.
 * - Stop words should be removed.
 * - If there's no possible way to break the string, throw an exception.
 */
public class WordBreakTokenizer implements Tokenizer {
    Map<String, Long> dictMap;
    long freqSum;
    int maxTokenLen;

    public WordBreakTokenizer() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer.class.getClassLoader().getResource("cs221_frequency_dictionary_en.txt");
            List<String> dictLines = Files.readAllLines(Paths.get(dictResource.toURI()));
            initializeMap(dictLines);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeMap(List<String> dictLines) {
        dictMap = new HashMap<>();
        String s, key;
        Long value;
        int k;
        freqSum = 0L;
        maxTokenLen = 0;
        //loop through the list and put it in a hashmap where key is the word and value is
        //the frequency for O(1) access in the breaking algorithm
        for (int i = 0; i < dictLines.size(); i++) {
            s = dictLines.get(i);
            if (s.startsWith("\uFEFF")) {
                s = s.substring(1);
            }
            k = s.indexOf(" ");
            key = s.substring(0, k);
            if (key.length() > maxTokenLen) {
                maxTokenLen = key.length();
            }
            value = Long.valueOf(s.substring(k + 1));
            freqSum += value;
            dictMap.put(key, value);
        }
    }

    /**
     * algorithm is taken from the slides' suggested youtube video
     * https://github.com/mission-peace/interview/blob/master/src/com/interview/dynamic/BreakMultipleWordsWithNoSpaceIntoSpace.java
     *
     * Changes Made:
     * 1. created a class WordBreakAnalyzer to keep for each segment the split positions
     * for each token and each segment probability
     * 2. return only the best segment and not all possible segments by comparing the probabilities by making
     * WordBreakAnalyzer implement Comparable
     * 3. also saving space by adding the result only if the list of segments is more than 1
     */
    private List<WordBreakAnalyzer> wordBreakUtil(String s, Map<Integer, List<WordBreakAnalyzer>> dp, int start) {
        if (start == s.length()) {
            return Collections.singletonList(null);
        }

        if (dp.containsKey(start)) {
            return dp.get(start);
        }

        List<WordBreakAnalyzer> segments= new ArrayList<>();
        for (int i = start; i < s.length() && i < start + maxTokenLen; i++) {
            String newWord = s.substring(start, i + 1);
            if (!dictMap.containsKey(newWord)) {
                continue;
            }
            List<WordBreakAnalyzer> result = wordBreakUtil(s, dp, i + 1);
            WordBreakAnalyzer newSegment;
            for (WordBreakAnalyzer segment : result) {
                newSegment = new WordBreakAnalyzer();
                newSegment.setProbability(Math.log((double)dictMap.get(newWord) / freqSum));
                if(segment != null)
                {
                    newSegment.setSplitPos(new ArrayList<>(segment.getSplitPos()));
                    newSegment.setProbability(newSegment.getProbability() + segment.getProbability());
                }
                newSegment.getSplitPos().add(0,start);
                segments.add(newSegment);
                Collections.sort(segments);//to sort segments based on the probability
                // although sort is expensive, but the list would always have two or less elements
                if(segments.size() > 1)
                {
                    segments.remove(0);//added this line to save memory space
                    // if text input is very long and contains many very short tokens
                }
            }
        }
        if(segments.size() != 0) {
            dp.put(start, segments);//don't add if list size is 0
        }
        return segments;
    }

    public List<String> tokenize(String text) {
        List<String> result = new ArrayList<>();
        if (text.length() == 0) {
            return result;
        }
        text = text.toLowerCase();
        Map<Integer, List<WordBreakAnalyzer>> dp = new HashMap<>();
        List<WordBreakAnalyzer> subResult = wordBreakUtil(text, dp, 0);
        if(subResult.size() == 0) {
            throw new UnsupportedOperationException("Text is not breakable");
        }
            List<Integer> splitPos = subResult.get(0).getSplitPos();
            String token;
            for (int i = 0; i < splitPos.size(); i++) {
                if(i == (splitPos.size() - 1))
                {
                    token = text.substring(splitPos.get(i));
                }
                else {
                    token = text.substring(splitPos.get(i), splitPos.get(i + 1));
                }
                if (!StopWords.stopWords.contains(token)) {
                    result.add(token);
                }
            }

        return result;

    }

}
