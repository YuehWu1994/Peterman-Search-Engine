package edu.uci.ics.cs221.analysis;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;




/**
 * Project 1, task 1: Implement a simple tokenizer based on punctuations and white spaces.
 *
 * For example: the text "I am Happy Today!" should be tokenized to ["happy", "today"].
 *
 * Requirements:
 *  - White spaces (space, tab, newline, etc..) and punctuations provided below should be used to tokenize the text.
 *  - White spaces and punctuations should be removed from the result tokens.
 *  - All tokens should be converted to lower case.
 *  - Stop words should be filtered out. Use the stop word list provided in `StopWords.java`
 *
 */
public class PunctuationTokenizer implements Tokenizer {

    public static Set<String> punctuations = new HashSet<>();
    static {
        punctuations.addAll(Arrays.asList(",", ".", ";", "?", "!"));//what about parentheses,
        // single and double quotes, brackets, percentage sign?
    }

    public PunctuationTokenizer() {

    }

    public List<String> tokenize(String text) {
        List<String> res = new ArrayList<String>();

        try{
            int l = text.length(), i = 0;
            while(i < l){
                String token;
                int j = i;

                while(j < l){
                    char ch = text.charAt(j);
                    if(ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || punctuations.contains(String.valueOf(ch)) || j == l-1){
                        if(j == l-1 && ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r' && !punctuations.contains(String.valueOf(ch)) ) j = l;

                        token = text.substring(i, j).toLowerCase();
                        //System.out.println(i);
                        //System.out.println(j);
                        // exclude stop word and empty string
                        if(token.length() != 0  && !StopWords.stopWords.contains(token)){
                            res.add(token);
                        }

                        i = j+1;
                        break;
                    }
                    ++j;
                }
            }
        } catch (Exception e) {
            //throw new UnsupportedOperationException("Punctuation Tokenizer Unimplemented");
            System.out.println("Punctuation Tokenizer Unimplemented");
        }

        return res;
    }
}
