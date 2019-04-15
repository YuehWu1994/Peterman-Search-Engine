package edu.uci.ics.cs221.analysis;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Project 1, task 2: Implement a Dynamic-Programming based Word-Break Tokenizer.
 *
 * Word-break is a problem where given a dictionary and a string (text with all white spaces removed),
 * determine how to break the string into sequence of words.
 * For example:
 * input string "catanddog" is broken to tokens ["cat", "and", "dog"]
 *
 * We provide an English dictionary corpus with frequency information in "resources/cs221_frequency_dictionary_en.txt".
 * Use frequency statistics to choose the optimal way when there are many alternatives to break a string.
 * For example,
 * input string is "ai",
 * dictionary and probability is: "a": 0.1, "i": 0.1, and "ai": "0.05".
 *
 * Alternative 1: ["a", "i"], with probability p("a") * p("i") = 0.01
 * Alternative 2: ["ai"], with probability p("ai") = 0.05
 * Finally, ["ai"] is chosen as result because it has higher probability.
 *
 * Requirements:
 *  - Use Dynamic Programming for efficiency purposes.
 *  - Use the the given dictionary corpus and frequency statistics to determine optimal alternative.
 *      The probability is calculated as the product of each token's probability, assuming the tokens are independent.
 *  - A match in dictionary is case insensitive. Output tokens should all be in lower case.
 *  - Stop words should be removed.
 *  - If there's no possible way to break the string, throw an exception.
 *
 */
public class WordBreakTokenizer implements Tokenizer {

    public static Map<String, Double> probability = new HashMap<>();

    public WordBreakTokenizer() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer.class.getClassLoader().getResource("cs221_frequency_dictionary_en.txt");
            List<String> dictLines = Files.readAllLines(Paths.get(dictResource.toURI()));


            double total = 0;

            for (String dictLine : dictLines) {
                String[] spl = dictLine.split(" ", 2);
                total += Double.parseDouble(spl[1]);
            }

            for (String dictLine : dictLines) {
                if(dictLine.startsWith("\uFEFF")) dictLine = dictLine.substring(1);
                String[] spl = dictLine.split(" ", 2);
                probability.put(spl[0], Double.parseDouble(spl[1])/total);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> tokenize(String text) {
        //throw new UnsupportedOperationException("Word Break Unimplemented");

        // check when text has a word not in dictionary and has spaces and question mark,
        String[] tok = text.split("[\\p{Punct}&&[^']]+");
        if(tok[0] != text) throw new UnsupportedOperationException();


        List<String> res = new ArrayList<String>();
        try {
            int n = text.length();
            double [][] wordProb = new double [n][n];
            int [][] endPoint = new int [n][n];

            for(int l = 1; l <= n; ++l){

                for(int left = 0; left+l-1 < n; ++left){
                    int right = left+l-1;

                    double mxFreq = 0;
                    int endP = -1;
                    // split to 2 substring
                    for(int i = left; i < right; ++i){

                        if(wordProb[left][i] != 0 && wordProb[i+1][right] != 0 && wordProb[left][i] * wordProb[i+1][right] > mxFreq){
                            //System.out.println(wordProb[left][i]);
                            //System.out.println(wordProb[i+1][right]);
                            endP = i;
                            mxFreq = wordProb[left][i] * wordProb[i+1][right];
                        }
                    }

                    // no split, consider as 1 string
                    String strWhole = text.substring(left, right+1).toLowerCase();
                    if(probability.containsKey(strWhole) && probability.get(strWhole) > mxFreq) {
                        System.out.println("whole");
                        endP = right;
                        mxFreq = probability.get(strWhole);

                    }

                    wordProb[left][right] = mxFreq;
                    endPoint[left][right] = endP;
//                    System.out.println(left);
//                    System.out.println(right);
//                    System.out.println(mxFreq);
//                    System.out.println(endP);
                }
            }

            // deal with unknown word
            if(wordProb[0][n-1] == 0.0){
                throw new UnsupportedOperationException();
            }

            recur(0, n-1, endPoint, text, res);

        } catch(Exception e){
            System.out.println("Word Break Fail");
        }

        return res;
    }

    public void recur(int startP, int endP, int [][] endPoint, String text, List<String> res){
        while(startP <= endP){
            int p = endPoint[startP][endP];
            if(p == endP){
                String token = text.substring(startP, p+1).toLowerCase();
                System.out.println(token);
                if(!StopWords.stopWords.contains(token)) {
                    res.add(token);
                }
            }
            else recur(startP, p, endPoint, text, res);

            startP = p+1;
        }
    }

}
