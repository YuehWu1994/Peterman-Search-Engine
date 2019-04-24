package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.storage.MapdbDocStore;
import edu.uci.ics.cs221.storage.Document;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

/**
 * This class manages an disk-based inverted index and all the documents in the inverted index.
 *
 * Please refer to the project 2 wiki page for implementation guidelines.
 */
public class InvertedIndexManager {

    /**
     * The default flush threshold, in terms of number of documents.
     * For example, a new Segment should be automatically created whenever there's 1000 documents in the buffer.
     *
     * In test cases, the default flush threshold could possibly be set to any number.
     */
    public static int DEFAULT_FLUSH_THRESHOLD = 1000;

    /**
     * The default merge threshold, in terms of number of segments in the inverted index.
     * When the number of segments reaches the threshold, a merge should be automatically triggered.
     *
     * In test cases, the default merge threshold could possibly be set to any number.
     */
    public static int DEFAULT_MERGE_THRESHOLD = 8;

    /**
     * Map keyword with list of document ID
     */
    public static Map<String, List<Integer>> keyWordMap = new HashMap<>();

    /**
     * DocStore Instance
     */
    public static MapdbDocStore mapDB;

    /**
     * Number of sequence in disk (for merge)
     */
    public static int NUM_SEQ = 0;

    /**
     * Document Counter (for flush)
     */
    public static Integer document_Counter;


    /**
     * Total length of keyword (in order to build dictionary on page file)
     */
    public static Integer totalLengthKeyword = 0;


    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        document_Counter = 0;
    }

    /**
     * Creates an inverted index manager with the folder and an analyzer
     */
    public static InvertedIndexManager createOrOpen(String indexFolder, Analyzer analyzer) {
        try {
            Path indexFolderPath = Paths.get(indexFolder);
            if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
                if (Files.isDirectory(indexFolderPath)) {
                    return new InvertedIndexManager(indexFolder, analyzer);
                } else {
                    throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
                }
            } else {
                Files.createDirectories(indexFolderPath);
                return new InvertedIndexManager(indexFolder, analyzer);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Adds a document to the inverted index.
     * Document should live in a in-memory buffer until `flush()` is called to write the segment to disk.
     * @param document
     */
    public void addDocument(Document document) {
        //throw new UnsupportedOperationException();

        // process (analyzer) text in the document
        PunctuationTokenizer tokenizer = new PunctuationTokenizer();
        PorterStemmer porterStemmer = new PorterStemmer();

        Analyzer analyzer = new ComposableAnalyzer(tokenizer, porterStemmer);
        List<String> word = analyzer.analyze(document.getText());


        // record on hashmap
        for(String w : word){
            if(!keyWordMap.containsKey(w)){
                keyWordMap.put(w, new ArrayList());
                totalLengthKeyword += w.length();
            }

            keyWordMap.get(w).add(document_Counter);
        }

        // add document into DocStore
        mapDB.createOrOpen("Doc_Store" + NUM_SEQ);
        mapDB.addDocument(document_Counter, document);
        mapDB.close();

        ++document_Counter;

        // check whether FLUSH_THRESHOLD is satisfied
        if(NUM_SEQ == DEFAULT_FLUSH_THRESHOLD) flush();
    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {
        // throw new UnsupportedOperationException();

        SegmentInDiskManager segMgr = new SegmentInDiskManager(Paths.get("./segment" + NUM_SEQ));

        // allocate the position on start point of keyword
        segMgr.allocateDictStart(totalLengthKeyword);

        // insert keyword, metadata, docID respectively
        for (Map.Entry<String, List<Integer>> entry : keyWordMap.entrySet()) {
            segMgr.insertKeyWord(entry.getKey());
        }

        // allocate the number of keyword on start point of dictionary
        segMgr.allocateNumberOfKeyWord(keyWordMap.size());

        for (Map.Entry<String, List<Integer>> entry : keyWordMap.entrySet()) {
            segMgr.insertListOfDocID(entry.getValue());
        }

        for (Map.Entry<String, List<Integer>> entry : keyWordMap.entrySet()) {
            segMgr.insertMetaDataSlot(entry.getKey().length(), entry.getValue().size()*Integer.BYTES);
        }


        segMgr.close();

        reset();
    }

    public void reset(){
        ++ NUM_SEQ;
        keyWordMap.clear();
        document_Counter = 0;
        totalLengthKeyword = 0;
    }

    /**
     * Merges all the disk segments of the inverted index pair-wise.
     */
    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);
        throw new UnsupportedOperationException();
    }

    /**
     * Performs a single keyword search on the inverted index.
     * You could assume the analyzer won't convert the keyword into multiple tokens.
     * If the keyword is empty, it should not return anything.
     *
     * @param keyword keyword, cannot be null.
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchQuery(String keyword) {
        Preconditions.checkNotNull(keyword);

        throw new UnsupportedOperationException();
    }

    /**
     * Performs an AND boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the AND query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchAndQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);

        throw new UnsupportedOperationException();
    }

    /**
     * Performs an OR boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the OR query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchOrQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);

        throw new UnsupportedOperationException();
    }

    /**
     * Iterates through all the documents in all disk segments.
     */
    public Iterator<Document> documentIterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * Deletes all documents in all disk segments of the inverted index that match the query.
     * @param keyword
     */
    public void deleteDocuments(String keyword) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the total number of segments in the inverted index.
     * This function is used for checking correctness in test cases.
     *
     * @return number of index segments.
     *
     * Q: used in disk or in-memory
     */
    public int getNumSegments() {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads a disk segment into memory based on segmentNum.
     * This function is mainly used for checking correctness in test cases.
     *
     * @param segmentNum n-th segment in the inverted index (start from 0).
     * @return in-memory data structure with all contents in the index segment, null if segmentNum don't exist.
     * Q: used in disk or in-memory
     */
    public InvertedIndexSegmentForTest getIndexSegment(int segmentNum) {
        throw new UnsupportedOperationException();
    }


}
