package edu.uci.ics.cs221.index.inverted;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.InvertedIndexSegmentForTest;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.print.Doc;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class Team12DeletedTest {
    InvertedIndexManager index;
    Analyzer analyzer = new ComposableAnalyzer(new PunctuationTokenizer(), new PorterStemmer());
    String path = "./index/Team12DeletedTest/";

    Document[] documents = new Document[] {
            new Document("Thanks to English, you will be able to talk with people who don’t speak your native language."),
            new Document("Conversing (talking) with others in English opens up a whole new world of opportunities."),
            new Document("Talking in English will also be adventurous because you will probably feel a little nervous and excited."),
            new Document("But if you push your English speaking comfort zone and just open your mouth"),
            new Document("you will feel so accomplished (proud) and motivated to keep learning!"),
            new Document("Plus, your English will improve a lot if you have more conversations"),
            new Document("So let’s get started! To help you on this trip."),
            new Document("We have put together a friendly guide to English conversation for beginners."),
            new Document("I've started a new diet with vegetables and I've had a terrible week."),
            new Document("filled with useful, basic phrases—from greetings and small talk to saying goodbye—that will take you on your first conversation adventure."),
            new Document("If you need a push to start having conversations in English, watch the clip below for motivation."),
            new Document("It may be an informal conversation with a friend or an acquaintance."),
            new Document("Or you may use a more formal dialogue when having an English conversation with a colleague."),
            new Document("Let’s start with informal greetings. Here is how you can say hello"),
            new Document("Remember that “good night” normally means that you are saying goodbye. It is also commonly used right before going to bed."),
            new Document("What if you have never met the person you are talking to before?")};

    @Before public void build() {
        index = InvertedIndexManager.createOrOpen(path, analyzer);
        InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD = 1;
    }

    @After public void delete() {
        File file = new File(path);
        String[] filelist = file.list();
        for(String f : filelist){
            File temp = new File(path, f);
            temp.delete();
        }
        file.delete();

        InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD = 1000;
        InvertedIndexManager.DEFAULT_MERGE_THRESHOLD = 8;
    }


    /*

        For this test we check if the number of documents is the same at the end of all the merges as what we inserted
        we also check to see if the number of occurrences of someone of the key words is the same as what we counted
        by hand. Rather than checking the number of segments this test is really trying to test the content of the
        final segment.
     */

    @Test public void test() {
        for (int i = 0; i < documents.length; ++i) {
            index.addDocument(documents[i]);
        }

        index.deleteDocuments("accomplish");
        index.deleteDocuments("friendli");
        index.deleteDocuments("basic");


        while (index.getNumSegments() != 1) {
            index.mergeAllSegments();
        }
        InvertedIndexSegmentForTest segment = index.getIndexSegment(0);
        Map<Integer, Document> docs = segment.getDocuments();
        Map<String, List<Integer>> invertedLists = segment.getInvertedLists();

        assert invertedLists.get("(proud)").size() == 0;
        assert invertedLists.get("learn").size() == 0;
        assert invertedLists.get("goodbye").size() == 0;
        assert invertedLists.get("us").size() == 2;
        assert invertedLists.get("convers").size() == 5;
        assert invertedLists.get("english").size() == 7;
    }

}