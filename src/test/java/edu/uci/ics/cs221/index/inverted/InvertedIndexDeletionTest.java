package edu.uci.ics.cs221.index.inverted;

import edu.uci.ics.cs221.analysis.*;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;


/**
 * For teams doing project 2 extra credits (deletion), please add all your own deletion test cases in this class.
 * The TA will only look at this class to give extra credit points.
 */
public class InvertedIndexDeletionTest {
    //test when the deleted is more than 1k
    //test multiple documents in a segment and only one document or two are deleted

    private String path = "./index/Team12DeleteTest";
    private Analyzer analyzer = new ComposableAnalyzer(new PunctuationTokenizer(), new PorterStemmer());
    private InvertedIndexManager invertedList;

    public void setUp() {
        File directory = new File(path);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        invertedList = InvertedIndexManager.createOrOpen(path, analyzer);
        invertedList.addDocument(new Document("cat dog toy"));
        invertedList.flush();
        invertedList.addDocument(new Document("cat Dot"));
        invertedList.flush();
        invertedList.addDocument(new Document("cat dot toy"));
        invertedList.flush();
        invertedList.addDocument(new Document("cat toy Dog"));
        invertedList.flush();
        invertedList.addDocument(new Document("toy dog cat"));
        invertedList.flush();
        invertedList.addDocument(new Document("cat Dog"));//docs cannot be null
        invertedList.flush();
        invertedList.addDocument(new Document("fish cat"));
        invertedList.flush();
        invertedList.addDocument(new Document("cat bird"));//docs cannot be null
        invertedList.flush();
    }

    //1. delete dog
    //2. get number of documents should be 8
    //3. dog should be in some documents
    //4. merge segments should have 2 segments
    //5. get all docs and check they don't contain dog
    //6. get number of documents should be 3
    @Test
    public void Test1() {
        setUp();
        invertedList.deleteDocuments("dog");
        Iterator<Document> docs = invertedList.documentIterator();
        int counter = 0;
        int numOfDocs = 0;
        while (docs.hasNext()) {
            numOfDocs++;
            String text = docs.next().getText().toLowerCase();
            if(text.contains("dog")) {
                counter++;
            }
        }
        Assert.assertEquals(4, counter);
        Assert.assertEquals(8, numOfDocs);
        invertedList.mergeAllSegments();
        Assert.assertEquals(2, invertedList.getNumSegments());
        docs = invertedList.documentIterator();
        counter = 0;
        numOfDocs = 0;
        while (docs.hasNext()) {
            numOfDocs++;
            String text = docs.next().getText().toLowerCase();
            if(text.contains("dog")) {
                counter++;
            }
        }
        Assert.assertEquals(0, counter);
        Assert.assertEquals(4, numOfDocs);
    }

    //test when the keyword is not found
    @Test
    public void Test2() {
        setUp();

        invertedList.deleteDocuments("elephant");
        Iterator<Document> docs = invertedList.documentIterator();
        int counter = 0;
        int numOfDocs = 0;
        while (docs.hasNext()) {
            numOfDocs++;
            String text = docs.next().getText().toLowerCase();
            if(text.contains("elephant")) {
                counter++;
            }
        }
        Assert.assertEquals(0, counter);
        Assert.assertEquals(8, numOfDocs);
        invertedList.mergeAllSegments();
        Assert.assertEquals(4, invertedList.getNumSegments());
        docs = invertedList.documentIterator();
        counter = 0;
        numOfDocs = 0;
        while (docs.hasNext()) {
            numOfDocs++;
            String text = docs.next().getText().toLowerCase();
            if(text.contains("elephant")) {
                counter++;
            }
        }
        Assert.assertEquals(0, counter);
        Assert.assertEquals(8, numOfDocs);
    }

    //test when the keyword is actually in all documents
    @Test
    public void Test3() {
        setUp();

        invertedList.deleteDocuments("cat");
        Iterator<Document> docs = invertedList.documentIterator();
        int counter = 0;
        int numOfDocs = 0;
        while (docs.hasNext()) {
            numOfDocs++;
            String text = docs.next().getText().toLowerCase();
            if(text.contains("cat")) {
                counter++;
            }
        }
        Assert.assertEquals(8, counter);
        Assert.assertEquals(8, numOfDocs);
        invertedList.mergeAllSegments();
        Assert.assertEquals(0, invertedList.getNumSegments());
        docs = invertedList.documentIterator();
        counter = 0;
        numOfDocs = 0;
        while (docs.hasNext()) {
            numOfDocs++;
            String text = docs.next().getText().toLowerCase();
            if(text.contains("cat")) {
                counter++;
            }
        }
        Assert.assertEquals(0, counter);
        Assert.assertEquals(0, numOfDocs);
    }


    @Test
    public void Test4()
    {
        File directory = new File(path);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        invertedList = InvertedIndexManager.createOrOpen(path, analyzer);
        invertedList.addDocument(new Document("cat dog toy"));
        invertedList.addDocument(new Document("cat Dot"));
        invertedList.flush();
        invertedList.addDocument(new Document("cat dot toy"));
        invertedList.addDocument(new Document("cat toy Dog"));
        invertedList.flush();
        invertedList.addDocument(new Document("toy dog cat"));
        invertedList.addDocument(new Document("cat Dog"));//docs cannot be null
        invertedList.flush();
        invertedList.addDocument(new Document("fish cat"));
        invertedList.addDocument(new Document("cat bird"));//docs cannot be null
        invertedList.flush();

        invertedList.deleteDocuments("dog");
        Iterator<Document> docs = invertedList.documentIterator();
        int counter = 0;
        int numOfDocs = 0;
        while (docs.hasNext()) {
            numOfDocs++;
            String text = docs.next().getText().toLowerCase();
            if(text.contains("dog")) {
                counter++;
            }
        }
        Assert.assertEquals(4, counter);
        Assert.assertEquals(8, numOfDocs);
        invertedList.mergeAllSegments();
        Assert.assertEquals(2, invertedList.getNumSegments());
        docs = invertedList.documentIterator();
        counter = 0;
        numOfDocs = 0;
        while (docs.hasNext()) {
            numOfDocs++;
            String text = docs.next().getText().toLowerCase();
            if(text.contains("dog")) {
                counter++;
            }
        }
        Assert.assertEquals(0, counter);
        Assert.assertEquals(4, numOfDocs);
    }

    @Test public void test() {
        File directory = new File(path);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        invertedList = InvertedIndexManager.createOrOpen(path, analyzer);
        invertedList.addDocument(new Document("Thanks to English, you will be able to talk with people who don’t speak your native language."));
        invertedList.addDocument(new Document("Conversing (talking) with others in English opens up a whole new world of opportunities."));
        invertedList.addDocument(new Document("Talking in English will also be adventurous because you will probably feel a little nervous and excited."));
        invertedList.addDocument(new Document("But if you push your English speaking comfort zone and just open your mouth"));
        invertedList.flush();
        invertedList.addDocument(new Document("you will feel so accomplished (proud) and motivated to keep learning!"));
        invertedList.addDocument(new Document("Plus, your English will improve a lot if you have more conversations"));
        invertedList.addDocument(new Document("So let’s get started! To help you on this trip."));
        invertedList.addDocument(new Document("We have put together a friendly guide to English conversation for beginners."));
        invertedList.flush();
        invertedList.addDocument(new Document("I've started a new diet with vegetables and I've had a terrible week."));
        invertedList.addDocument(new Document("filled with useful, basic phrases—from greetings and small talk to saying goodbye—that will take you on your first conversation adventure."));
        invertedList.addDocument(new Document("If you need a push to start having conversations in English, watch the clip below for motivation."));
        invertedList.addDocument(new Document("It may be an informal conversation with a friend or an acquaintance."));
        invertedList.flush();
        invertedList.addDocument(new Document("Or you may use a more formal dialogue when having an English conversation with a colleague."));
        invertedList.addDocument(new Document("Let’s start with informal greetings. Here is how you can say hello"));
        invertedList.addDocument(new Document("Remember that “good night” normally means that you are saying goodbye. It is also commonly used right before going to bed."));
        invertedList.addDocument(new Document("What if you have never met the person you are talking to before?"));
        invertedList.flush();


        invertedList.deleteDocuments("accomplish");
        invertedList.deleteDocuments("friendli");
        invertedList.deleteDocuments("basic");


        while (invertedList.getNumSegments() != 1) {
            invertedList.mergeAllSegments();
        }
        InvertedIndexSegmentForTest segment = invertedList.getIndexSegment(0);
        Map<Integer, Document> docs = segment.getDocuments();
        Map<String, List<Integer>> invertedLists = segment.getInvertedLists();

        assert invertedLists.get("(proud)").size() == 0;
        assert invertedLists.get("learn").size() == 0;
        assert invertedLists.get("goodbye").size() == 0;
        assert invertedLists.get("us").size() == 2;
        assert invertedLists.get("convers").size() == 5;
        assert invertedLists.get("english").size() == 7;
    }
    @After

    public void deleteTmp() {
        PageFileChannel.resetCounters();
        File f = new File(path);
        File[] files = f.listFiles();
        for (File file : files) {
            file.delete();
        }
        f.delete();
    }
}