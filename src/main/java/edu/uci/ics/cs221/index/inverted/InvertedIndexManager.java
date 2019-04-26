package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.storage.DocumentStore;
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
import java.io.File;

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
        DocumentStore mapDB = MapdbDocStore.createOrOpen("Doc_Store" + NUM_SEQ); // not sure
        mapDB.addDocument(document_Counter, document);
        mapDB.close();

        ++document_Counter;

        if(document_Counter == DEFAULT_FLUSH_THRESHOLD) flush();

        if(NUM_SEQ == DEFAULT_MERGE_THRESHOLD) mergeAllSegments();
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
            segMgr.insertMetaDataSlot(entry.getKey().length(), entry.getValue().size()*Integer.BYTES);
        }

        for (Map.Entry<String, List<Integer>> entry : keyWordMap.entrySet()) {
            segMgr.insertListOfDocID(entry.getValue());
        }

        segMgr.allocateDocIDEnd();

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
        //throw new UnsupportedOperationException();

        String seg1 = "";
        File p = new File(".");
        String[] entries = p.list();
        // assume to file is ordered !!!!!!!!!!!!!!!!!! not sure (naming problem)
        for (int i = 0; i < entries.length; ++i) {
            if(entries[i].substring(0,7) == "segment"){
                if(seg1 != ""){
                    // merge
                    merge(seg1, entries[i]);

                    // after merge
                    seg1 = "";
                }
                else seg1 = entries[i];
            }
        }
    }

    public void merge(String seg1, String seg2){
        // get segment id and docId size
        int sz1, sz2;
        int id1 = Integer.parseInt(seg1.substring(7));
        int id2 = Integer.parseInt(seg2.substring(7));


        DocumentStore mapDB1 = MapdbDocStore.createOrOpen("Doc_Store" + id1);
        sz1 = (int)mapDB1.size();


        DocumentStore mapDB2 = MapdbDocStore.createOrOpen("Doc_Store" + id2);
        sz2 = (int)mapDB2.size();


        /*
         * create map to store keyword and dictionary pair, the list either contain 4 attributes or 8 attributes
         * Specification of value at Map : segId(either 0,1) | page | offset | length  , stored at List of integer
         * If the keyword exist in both segments, the list would have two dictionary (8 attribute)
         */
        Map<String, List<Integer>> mergedMap = new HashMap<>();

        SegmentInDiskManager segMgr1 = new SegmentInDiskManager(Paths.get("./segment" + id1));
        SegmentInDiskManager segMgr2 = new SegmentInDiskManager(Paths.get("./segment" + id2));
        segMgr1.readInitiate(); segMgr2.readInitiate();


        // read to fill the map
        int totalLengthKeyword = fillTheMap(mergedMap, segMgr1, segMgr2);

        SegmentInDiskManager segMgrMerge = new SegmentInDiskManager(Paths.get("./mergedSegment"));

        // insert to new segment
        insertAtMergedSegment(mergedMap, segMgr1, segMgr2, segMgrMerge, totalLengthKeyword, sz1);

        // add documentStore at doc1 toward doc0
        for(int i = 0 ; i < sz2; ++i){
            Document d = mapDB2.getDocument(0);
            mapDB1.addDocument(i+sz1, d);
        }
        mapDB1.close();
        mapDB2.close();


        // close
        segMgr1.close();
        segMgr2.close();
        segMgrMerge.close();


        deleteAndRename(id1, id2);
    }

    // Specification of value at Map : segId(either 0,1) | page | offset | length  , stored at List of integer
    public int fillTheMap(Map<String, List<Integer>> mergedMap, SegmentInDiskManager segMgr1, SegmentInDiskManager segMgr2){
        int totalLengthKeyword = 0;
        while(segMgr1.hasKeyWord() || segMgr2.hasKeyWord()){
            String k1 = "", k2 = "";
            List<Integer> l1 = new ArrayList<>(), l2 = new ArrayList<>();

            if(segMgr1.hasKeyWord()){
                k1 = segMgr1.readKeywordAndDict(l1);
                l1.add(0, 0);
            }
            if(segMgr2.hasKeyWord()){
                k2 = segMgr2.readKeywordAndDict(l2);
                l2.add(0, 1);
            }

            int cmp = k1.compareTo(k2);
            if(cmp == 0){
                totalLengthKeyword += k1.length();
                l1.addAll(l2);
                mergedMap.put(k1, l1);
                segMgr1.nextDict();
                segMgr2.nextDict();
            }
            else if(k2.isEmpty() || (cmp < 0 && !k1.isEmpty() )){
                totalLengthKeyword += k1.length();
                mergedMap.put(k1, l1);
                segMgr1.nextDict();
            }
            else{
                totalLengthKeyword += k2.length();
                mergedMap.put(k2, l2);
                segMgr2.nextDict();
            }
        }
        return totalLengthKeyword;
    }


    public void insertAtMergedSegment(Map<String, List<Integer>> mergedMap, SegmentInDiskManager segMgr1, SegmentInDiskManager segMgr2, SegmentInDiskManager segMgrMerge, int totalLengthKeyword, int sz1){
        // allocate the position on start point of keyword
        segMgrMerge.allocateDictStart(totalLengthKeyword);


        // insert keyword, metadata, docID respectively
        for (Map.Entry<String, List<Integer>> entry : mergedMap.entrySet()) {
            segMgrMerge.insertKeyWord(entry.getKey());
        }


        // allocate the number of keyword on start point of dictionary
        segMgrMerge.allocateNumberOfKeyWord(mergedMap.size());

        for (Map.Entry<String, List<Integer>> entry : mergedMap.entrySet()) {
            int docIdLength =  entry.getValue().get(3);

            // if this key word exists in both segments
            if(entry.getValue().size() == 8) docIdLength += entry.getValue().get(7);

            segMgrMerge.insertMetaDataSlot(entry.getKey().length(), docIdLength);
        }


        // read docId from segment 1, 2 and write to new segment
        for (Map.Entry<String, List<Integer>> entry : mergedMap.entrySet()) {
            List<Integer> docIdList1 = new ArrayList<>(), docIdList2 = new ArrayList<>();

            List<Integer> v = entry.getValue();

            // the keyword exist in both segments
            if(v.size() == 8){
                docIdList1 = segMgr1.readDocIdList(v.get(3));
                docIdList2 = segMgr2.readDocIdList(v.get(7));
            }
            else{
                // exist in either  1st/2nd segment
                if(v.get(0) == 0)  docIdList1 = segMgr1.readDocIdList(v.get(3));
                else docIdList2 = segMgr2.readDocIdList(v.get(3));
            }

            // convert docId in segment 2
            for(int i : docIdList2) i += sz1;

            // concat docId2 to docId1
            docIdList1.addAll(docIdList2);

            segMgrMerge.insertListOfDocID(docIdList1);
        }


        segMgrMerge.allocateDocIDEnd();
    }

    public void deleteAndRename(int id1, int id2){
        // delete segment
        File f1, f2;
        f1 = new File("./segment" + id1);
        f2 = new File("./segment" + id2);
        f1.delete(); f2.delete();

        // rename segment
        f1 = new File("./mergedSegment");
        f2 = new File("./segment" + id1/2);
        boolean success = f1.renameTo(f2);

        if(!success) throw new UnsupportedOperationException("rename segment fail");


        // delete 2nd document store
        f2 = new File("Doc_Store" + id2);
        f2.delete();

        // rename 1st document store
        f1 = new File("Doc_Store" + id1);
        f2 = new File("Doc_Store" + id1/2);
        success = f1.renameTo(f2);

        if(!success) throw new UnsupportedOperationException("rename docstore fail");
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
        int cnt = 0;
        File p = new File(".");
        String[] entries = p.list();
        for (int i = 0; i < entries.length; ++i) {
            if(entries[i].substring(0,7) == "segment") ++cnt;
        }
        return cnt;
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
