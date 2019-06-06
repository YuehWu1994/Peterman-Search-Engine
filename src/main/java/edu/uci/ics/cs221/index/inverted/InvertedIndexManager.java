package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;
import edu.uci.ics.cs221.storage.Document;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.ArrayList;
import java.io.File;
import java.util.stream.Collectors;


/**
 * This class manages an disk-based inverted index and all the documents in the inverted index.
 * <p>
 * Please refer to the project 2 wiki page for implementation guidelines.
 */
public class InvertedIndexManager {

    /**
     * The default flush threshold, in terms of number of documents.
     * For example, a new Segment should be automatically created whenever there's 1000 documents in the buffer.
     * <p>
     * In test cases, the default flush threshold could possibly be set to any number.
     */
    public static int DEFAULT_FLUSH_THRESHOLD = 1000;

    /**
     * The default merge threshold, in terms of number of segments in the inverted index.
     * When the number of segments reaches the threshold, a merge should be automatically triggered.
     * <p>
     * In test cases, the default merge threshold could possibly be set to any number.
     */
    public static int DEFAULT_MERGE_THRESHOLD = 8;

    /**
     * Map keyword with list of document ID
     */
    private Table<String, Integer, List<Integer>> keyWordMap;


    private DocumentStore mapDB;

    /**
     * Number of sequence in disk (for merge)
     */
    private int NUM_SEQ;

    /**
     * Document Counter (for flush)
     */
    private Integer document_Counter;


    /**
     * Total length of keyword (in order to build dictionary on page file)
     */
    private Integer totalLengthKeyword;


    private String idxFolder;

    private Analyzer iiAnalyzer;

    private Compressor iiCompressor;

    private enum SearchOperation {
        AND_SEARCH,
        OR_SEARCH
    }


    // ranking param
    private Map<String, List<Integer>> rankingDictMap;

    private Integer rankingSegId;


    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        document_Counter = 0;
        idxFolder = indexFolder + "/";
        NUM_SEQ = 0;
        document_Counter = 0;
        totalLengthKeyword = 0;
        keyWordMap = TreeBasedTable.create();
        iiAnalyzer = analyzer;
        rankingDictMap = new TreeMap<>();
        rankingSegId = -1;
    }

    /**
     * ADDED THIS CONSTRUCTOR TO SOLVE ISSUE OF STATIC COMPRESSOR
     *
     * @param indexFolder
     * @param analyzer
     */
    private InvertedIndexManager(String indexFolder, Analyzer analyzer, Compressor compressor) {
        document_Counter = 0;
        iiCompressor = compressor;
        idxFolder = indexFolder + "/";
        NUM_SEQ = 0;
        document_Counter = 0;
        totalLengthKeyword = 0;
        keyWordMap = TreeBasedTable.create();
        iiAnalyzer = analyzer;
        rankingDictMap = new TreeMap<>();
        rankingSegId = -1;
    }

    /**
     * Creates an inverted index manager with the folder and an analyzer
     */
    public static InvertedIndexManager createOrOpen(String indexFolder, Analyzer analyzer) {

        try {

            Path indexFolderPath = Paths.get(indexFolder);
            if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
                if (Files.isDirectory(indexFolderPath)) {
                    return new InvertedIndexManager(indexFolder, analyzer, null);
                } else {
                    throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
                }
            } else {
                Files.createDirectories(indexFolderPath);
                return new InvertedIndexManager(indexFolder, analyzer, null);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Creates a positional index with the given folder, analyzer, and the compressor.
     * Compressor must be used to compress the inverted lists and the position lists.
     */
    public static InvertedIndexManager createOrOpenPositional(String indexFolder, Analyzer analyzer, Compressor compressor) {
        try {
            Path indexFolderPath = Paths.get(indexFolder);
            if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
                if (Files.isDirectory(indexFolderPath)) {
                    return new InvertedIndexManager(indexFolder, analyzer, compressor);
                } else {
                    throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
                }
            } else {
                Files.createDirectories(indexFolderPath);
                return new InvertedIndexManager(indexFolder, analyzer, compressor);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Adds a document to the inverted index.
     * Document should live in a in-memory buffer until `flush()` is called to write the segment to disk.
     *
     * @param document
     */
    public void addDocument(Document document) {
        // process (analyzer) text in the document
        List<String> word = iiAnalyzer.analyze(document.getText());
        List<Integer> positions;
        int wordPosition = 0;
        // record on hashmap
        for (String w : word) {
            if (!keyWordMap.containsRow(w)) {
                totalLengthKeyword += w.getBytes().length;
            }
            positions = keyWordMap.get(w, document_Counter);
            if (positions == null) {
                positions = new ArrayList<>();
            }
            positions.add(wordPosition++);
            keyWordMap.put(w, document_Counter, positions);
        }

        // add document into DocStore

        File f = new File(idxFolder + "DocStore_" + NUM_SEQ);
        if (!f.exists()) {
            mapDB = MapdbDocStore.createOrOpen(idxFolder + "DocStore_" + NUM_SEQ);
        }

        mapDB.addDocument(document_Counter, document);


        ++document_Counter;

        if (document_Counter == DEFAULT_FLUSH_THRESHOLD) {
            flush();
        }

    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {
        if (document_Counter == 0) {
            return;
        }

        SegmentInDiskManager segMgr = new SegmentInDiskManager(idxFolder, Integer.toString(NUM_SEQ), iiCompressor);


        // allocate dictionary bytebuffer
        segMgr.allocateByteBuffer(totalLengthKeyword, keyWordMap.rowMap().size());


        // allocate the position on start point of keyword
        segMgr.allocateKeywordStart(totalLengthKeyword);

        // insert keyword, metadata, docID in one pass
        for (Map.Entry<String, Map<Integer, List<Integer>>> entry : keyWordMap.rowMap().entrySet()) {
            segMgr.insertKeyWord(entry.getKey());
            byte[] encodedPostingList;
            if (isPositionalIndex()) {
                encodedPostingList = iiCompressor.encode(entry.getValue().keySet().stream().collect(Collectors.toCollection(ArrayList::new)));
            } else {
                encodedPostingList = new NaiveCompressor().encode(entry.getValue().keySet().stream().collect(Collectors.toCollection(ArrayList::new)));
            }
            segMgr.insertMetaDataSlot(entry.getKey().getBytes().length, encodedPostingList.length, entry.getValue().size());
            segMgr.insertPostingList(encodedPostingList);

            //iterate through every documentID and get the position list
                for (Map.Entry<Integer, List<Integer>> docId : entry.getValue().entrySet()) {
                    segMgr.insertTFList(docId.getValue().size());
                    if (isPositionalIndex()) {
                        byte[] encodedPositionList;
                    encodedPositionList = iiCompressor.encode(docId.getValue());
                    segMgr.insertPositionList(encodedPositionList, docId.getValue().size());
                }
            }

        }

        // allocate the number of keyword on start point of dictionary
        segMgr.allocateNumberOfKeyWord(keyWordMap.rowMap().size());


        // append all dictionary byte to new file
        segMgr.appendAllbyte();
        segMgr.appendPage();

        segMgr.close();

        reset();

        if (NUM_SEQ == DEFAULT_MERGE_THRESHOLD) {
            mergeAllSegments();
        }
    }


    /**
     * Merges all the disk segments of the inverted index pair-wise.
     */
    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);
        String seg1 = "";
        File[] files = getFiles("segment");

        sort(files);

        for (int i = 0; i < files.length; ++i) {
            if (seg1 != "") {
                // merge
                merge(Integer.parseInt(seg1.substring(8)), Integer.parseInt(files[i].getName().substring(8)));

                // after merge
                seg1 = "";
            } else {
                seg1 = files[i].getName();
            }
        }

        // minus NUM_SEQ by half
        NUM_SEQ = NUM_SEQ / 2;
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
        List<Document> iterator = new ArrayList<>();
        if (keyword.equals("")) {
            return iterator.iterator();
        }

        return searchKewords(iiAnalyzer.analyze(keyword), SearchOperation.AND_SEARCH);
    }

    /**
     * Performs an AND boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the AND query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchAndQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);
        List<Document> iterator = new ArrayList<>();
        if (keywords.isEmpty() || keywords.contains("")) {
            return iterator.iterator();
        }
        List<String> words = new ArrayList<>();
        for (int i = 0; i < keywords.size(); i++) {
            words.addAll(iiAnalyzer.analyze(keywords.get(i)));
        }
        return searchKewords(Lists.newArrayList(words), SearchOperation.AND_SEARCH);
    }

    /**
     * Performs an OR boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the OR query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchOrQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);
        List<Document> iterator = new ArrayList<>();
        if (keywords.isEmpty()) {
            return iterator.iterator();
        }
        List<String> words = new ArrayList<>();
        for (int i = 0; i < keywords.size(); i++) {
            words.addAll(iiAnalyzer.analyze(keywords.get(i)));
        }
        return searchKewords(Lists.newArrayList(words), SearchOperation.OR_SEARCH);
    }

    /**
     * Performs a phrase search on a positional index.
     * Phrase search means the document must contain the consecutive sequence of keywords in exact order.
     * <p>
     * You could assume the analyzer won't convert each keyword into multiple tokens.
     * Throws UnsupportedOperationException if the inverted index is not a positional index.
     *
     * @param phrase, a consecutive sequence of keywords
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchPhraseQuery(List<String> phrase) {
        Preconditions.checkNotNull(phrase);
        List<Document> iterator = new ArrayList<>();

        if (!isPositionalIndex()) {
            throw new UnsupportedOperationException();
        }

        if (phrase.isEmpty()) {
            return iterator.iterator();
        }

        //concat the list of phrases
        //do analyzer
        List<String> keywords = iiAnalyzer.analyze(String.join(" ", phrase));
        File[] files = getFiles("segment");
        sort(files);
        //loop through every segment
        for (int i = 0; i < files.length; ++i) {

            SegmentInDiskManager segMgr = new SegmentInDiskManager(idxFolder, files[i].getName().substring(8), iiCompressor);
            segMgr.readInitiate();
            Map<String, List<Integer>> dictMap = new TreeMap<>();
            //load all the keywords of the segment
            while (segMgr.hasKeyWord()) {
                List<Integer> l1 = new ArrayList<>();
                String k1 = segMgr.readKeywordAndDict(l1);
                dictMap.put(k1, l1);
            }
            //if keywords in segment doesn't contain query then continue
            if (!dictMap.keySet().containsAll(keywords)) {
                continue;
            }
            //loop through every token from query
            Map<Integer, List<Integer>> postingList1, postingList2;
            segMgr.readPostingInitiate();
            segMgr.readPositionMetaInitiate();
            segMgr.readPositionInitiate();


            //get all docs and its metadata of the first token
            postingList1 = segMgr.readDocIdList(dictMap.get(keywords.get(0)).get(0),
                    dictMap.get(keywords.get(0)).get(1), dictMap.get(keywords.get(0)).get(2), dictMap.get(keywords.get(0)).get(3));
            for (int j = 1; j < keywords.size(); j++) {
                //inside the loop get the next token from phrase
                //then get all docs of keyword two
                Map<Integer, List<Integer>> tmpPostingList = new TreeMap<>();


                postingList2 = segMgr.readDocIdList(dictMap.get(keywords.get(j)).get(0),
                        dictMap.get(keywords.get(j)).get(1), dictMap.get(keywords.get(j)).get(2), dictMap.get(keywords.get(j)).get(3));
                //loop through all docs of keword 1
                for (Map.Entry<Integer, List<Integer>> entry : postingList1.entrySet()) {
                    //if docid is not found in second keyword then continue
                    if (!postingList2.containsKey(entry.getKey())) {
                        continue;
                    }
                    // else get the positional of both
                    List<Integer> positionalList1 = segMgr.readPosList(entry.getValue().get(0), entry.getValue().get(1),
                            entry.getValue().get(2));
                    List<Integer> positionalList2 = segMgr.readPosList(postingList2.get(entry.getKey()).get(0), postingList2.get(entry.getKey()).get(1),
                            postingList2.get(entry.getKey()).get(2));

                    //loop through every position in doc1 of keyword1
                    int pos = 0;
                    boolean found = false;
                    for (int k = 0; k < positionalList1.size() && pos < positionalList2.size(); k++) {
                        while (pos < positionalList2.size() && positionalList1.get(k) > positionalList2.get(pos)) {
                            pos++;
                        }

                        if (pos < positionalList2.size() && positionalList1.get(k) + j == positionalList2.get(pos)) {
                            found = true;
                            break;
                        }
                    }

                    // if found the position of two words are consecutive, store the docId and its metadata of the second word
                    if(found) {
                        List<Integer> lst = new ArrayList<>();
                        lst.add(entry.getValue().get(0));
                        lst.add(entry.getValue().get(1));
                        lst.add(entry.getValue().get(2));
                        tmpPostingList.put(entry.getKey(), lst);
                    }
                }
                postingList1 = tmpPostingList;
            }

            //System.out.println(postingList1);
            DocumentStore mapDBGetIdx = MapdbDocStore.createOrOpenReadOnly(idxFolder + "DocStore_" + files[i].getName().substring(8));

            for (Map.Entry<Integer, List<Integer>> entry : postingList1.entrySet()){
                iterator.add(mapDBGetIdx.getDocument(entry.getKey()));
            }
            mapDBGetIdx.close();
            segMgr.close();
        }
        return iterator.iterator();

    }


    /**
     * Iterates through all the documents in all disk segments.
     */
    public Iterator<Document> documentIterator() {
        Iterator<Document> iterator = new ArrayList<Document>().iterator();

        File[] files = getFiles("DocStore");
        for (int i = 0; i < files.length; ++i) {
            DocumentStore mapDBIt = MapdbDocStore.createOrOpenReadOnly(files[i].getPath());
            iterator = Iterators.concat(iterator, Iterators.transform(mapDBIt.iterator(), entry -> entry.getValue()));
            mapDBIt.close();
        }
        return iterator;
    }

    /**
     * Deletes all documents in all disk segments of the inverted index that match the query.
     *
     * @param keyword
     */
    public void deleteDocuments(String keyword) {
    }


    /**
     * Performs top-K ranked search using TF-IDF.
     * Returns an iterator that returns the top K documents with highest TF-IDF scores.
     *
     * Each element is a pair of <Document, Double (TF-IDF Score)>.
     *
     * If parameter `topK` is null, then returns all the matching documents.
     *
     * Unlike Boolean Query and Phrase Query where order of the documents doesn't matter,
     * for ranked search, order of the document returned by the iterator matters.
     *
     * @param keywords, a list of keywords in the query
     * @param topK, number of top documents weighted by TF-IDF
     * @return a iterator of ordered documents matching the query
     */
    public Iterator<Pair<Document, Double>> searchTfIdf(List<String> keywords, Integer topK)  {

        // check empty and positional index
        Preconditions.checkNotNull(keywords);
        Iterator<Pair<Document, Double>> iterator = new ArrayList<Pair<Document, Double>>().iterator();


        if (keywords.isEmpty()) {
            return iterator;
        }

        Map<String, Double> idf = new HashMap<>();

        // do analyzer
        List<String> tokens = iiAnalyzer.analyze(String.join(" ", keywords));

        // retrieve segment files
        File[] files = getFiles("segment");
        sort(files);


        // ### FIRST PASS: calculate IDF's of the query keywords
        setIDF(tokens, files, idf);

        // ### SECOND PASS: calculate the score
        List<ScoreSet> topKDocumentId = calculateScore(tokens, files, idf, topK);


        // setup document iterator
        for (int i = 0; i < topKDocumentId.size(); ++i) {
            DocumentStore mapDBIt = MapdbDocStore.createOrOpenReadOnly(idxFolder + "DocStore_" + topKDocumentId.get(i).Doc.SegmentID);

            List<Pair<Document, Double>> pairIt = new ArrayList<>();

            pairIt.add(new Pair(mapDBIt.getDocument(topKDocumentId.get(i).Doc.LocalDocID),  topKDocumentId.get(i).Score));
            iterator = Iterators.concat(iterator, pairIt.iterator());
            mapDBIt.close();
        }
        return iterator;
    }


    public void setIDF(List<String> tokens, File[] files, Map<String, Double> idf){
        //loop through every segment
        int numDoc = 0;

        for (int i = 0; i < files.length; ++i) {
            String fileIdxStr = files[i].getName().substring(8);

            // get/accumulate number of document
            numDoc += getNumDocuments(Integer.parseInt(fileIdxStr));


            // get/accumulate document frequency
            for(String w : tokens){
                int docFreq = getDocumentFrequency(Integer.parseInt(fileIdxStr), w);
                if(!idf.containsKey(w)) idf.put(w, (double)docFreq);
                else idf.put(w, idf.get(w) + (double)docFreq);
            }
        }

        // after looping all the segment, count global IDF of each keyword
        for(Map.Entry<String, Double> entry : idf.entrySet()){
            Double v = entry.getValue();
            entry.setValue(Math.log10(numDoc/v));
        }
    }

    public List<ScoreSet> calculateScore(List<String> tokens, File[] files, Map<String, Double> idf, Integer topK){
        PriorityQueue<ScoreSet> pq = new PriorityQueue<>();

        // setup tfidf of query
        Map<String, Double> queryTfidf = setTfidfQuery(tokens, idf);


        // loop all the segment file
        for (int i = 0; i < files.length; ++i) {
            String fileIdxStr = files[i].getName().substring(8);

            Map<Integer, Double> dotProductAccumulator = new HashMap<>();
            Map<Integer, Double> vectorLengthAccumulator = new HashMap<>();

            SegmentInDiskManager segMgr = new SegmentInDiskManager(idxFolder, fileIdxStr, iiCompressor);
            segMgr.readInitiate();
            Map<String, List<Integer>> dictMap = new TreeMap<>();

            //load all the keywords of the segment
            while (segMgr.hasKeyWord()) {
                List<Integer> l1 = new ArrayList<>();
                String k1 = segMgr.readKeywordAndDict(l1);
                dictMap.put(k1, l1);
            }

            // initiate
            segMgr.readPostingInitiate();
            segMgr.readPositionMetaInitiate();
            segMgr.readPositionInitiate();

            segMgr.readTFInitiate();

            // calculate tfidf and accumulate cosine similarity
            for(Map.Entry<String, Double> entry : queryTfidf.entrySet()){
                String w = entry.getKey();
                // p4 check!!!
                if(!dictMap.keySet().contains(w)){
                    continue;
                }
                Map<Integer, List<Integer>> postingList = segMgr.readDocIdList(dictMap.get(w).get(0), dictMap.get(w).get(1), dictMap.get(w).get(2), dictMap.get(w).get(3));

                for (Map.Entry<Integer, List<Integer>> postEntry : postingList.entrySet()) {
                    // use tf list file
                    int tf = postEntry.getValue().get(postEntry.getValue().size()-1);
                    Double tfidf = tf * idf.get(w);

                    int docId = postEntry.getKey();

                    if(!dotProductAccumulator.containsKey(docId)){
                        dotProductAccumulator.put(docId, tfidf* queryTfidf.get(w));
                        vectorLengthAccumulator.put(docId, tfidf*tfidf);
                    }
                    else{
                        dotProductAccumulator.put(docId, dotProductAccumulator.get(docId) + tfidf* queryTfidf.get(w));
                        vectorLengthAccumulator.put(docId, vectorLengthAccumulator.get(docId) + tfidf* tfidf);
                    }
                }

            }

            // retrieve the score and put scoreSet object into priority queue
            int segNumDoc = getNumDocuments(Integer.parseInt(fileIdxStr));
            for(int j = 0; j < segNumDoc; ++j){
                ScoreSet ss;

                if(!dotProductAccumulator.containsKey(j) || (dotProductAccumulator.get(j) == 0.0 && vectorLengthAccumulator.get(j) == 0.0)) continue;

                ss = new ScoreSet(dotProductAccumulator.get(j)/ Math.sqrt(vectorLengthAccumulator.get(j)), new DocID(Integer.parseInt(fileIdxStr), j));
                pq.add(ss);

                // check null
                if(topK != null && pq.size() > topK){
                    pq.poll(); // assume that the peek value is the smallest one !!!
                }

            }
        }

        // transform to docID list
        List<ScoreSet> scoreSetlist = new ArrayList<>();
        while(!pq.isEmpty()){
            ScoreSet ss = pq.poll();
            scoreSetlist.add(ss);
        }
        Collections.reverse(scoreSetlist);

        return scoreSetlist;
    }


    public Map<String, Double> setTfidfQuery(List<String> tokens, Map<String, Double> idf){
        Map<String, Double> queryTfidf = new HashMap<>();

        for(String w : tokens){
            if(!queryTfidf.containsKey(w)) queryTfidf.put(w, 1.0);
            else  queryTfidf.put(w, queryTfidf.get(w) + 1.0);
        }

        for(Map.Entry<String, Double> entry : queryTfidf.entrySet()){
            String w = entry.getKey();
            if(!idf.containsKey(w)) entry.setValue(0.0);
            else{
                Double tf = entry.getValue();
                entry.setValue(tf* idf.get(w));
            }
        }

        return queryTfidf;
    }

    /**
     * Returns the total number of documents within the given segment.
     */
    public int getNumDocuments(int segmentNum) {
        DocumentStore mapDBGetIdx = MapdbDocStore.createOrOpenReadOnly(idxFolder + "DocStore_" + segmentNum);
        int sz = (int)mapDBGetIdx.size();
        mapDBGetIdx.close();
        return sz;
    }

    /**
     * Returns the number of documents containing the token within the given segment.
     * The token should be already analyzed by the analyzer. The analyzer shouldn't be applied again.
     */
    public int getDocumentFrequency(int segmentNum, String token) {
        // check whether we should create new dictMap
        if(segmentNum != rankingSegId){
            resetRankingParam(segmentNum);
        }

        if(!rankingDictMap.keySet().contains(token)){
            return 0;
        }
        return rankingDictMap.get(token).get(4); // 5th element is the number of Docs
    }

    public void resetRankingParam(int segmentNum){
        rankingSegId = segmentNum;
        rankingDictMap.clear();

        SegmentInDiskManager segMgr = new SegmentInDiskManager(idxFolder, String.valueOf(rankingSegId), iiCompressor);
        segMgr.readInitiate();

        rankingDictMap = new TreeMap<>();

        //load all the keywords of the segment
        while (segMgr.hasKeyWord()) {
            List<Integer> l1 = new ArrayList<>();
            String k1 = segMgr.readKeywordAndDict(l1);
            rankingDictMap.put(k1, l1);
        }

        segMgr.close();
    }



    /**
     * Gets the total number of segments in the inverted index.
     * This function is used for checking correctness in test cases.
     *
     * @return number of index segments.
     * <p>
     * Q: used in disk or in-memory
     */
    public int getNumSegments() {
        return getFiles("segment").length;
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

        if (!Files.exists(Paths.get(idxFolder + "segment_" + segmentNum))) {
            return null;
        }
        Map<String, List<Integer>> invertedLists = new TreeMap<>();
        Map<Integer, Document> documents = new HashMap<>();
        getSegment(segmentNum, false, invertedLists, documents, null);
        return new InvertedIndexSegmentForTest(invertedLists, documents);
    }

    /**
     * Reads a disk segment of a positional index into memory based on segmentNum.
     * This function is mainly used for checking correctness in test cases.
     * <p>
     * Throws UnsupportedOperationException if the inverted index is not a positional index.
     *
     * @param segmentNum n-th segment in the inverted index (start from 0).
     * @return in-memory data structure with all contents in the index segment, null if segmentNum don't exist.
     */
    public PositionalIndexSegmentForTest getIndexSegmentPositional(int segmentNum) {
        if (!Files.exists(Paths.get(idxFolder + "segment_" + segmentNum))) {
            return null;
        }
        if (!Files.exists(Paths.get(idxFolder + "position_" + segmentNum))) {
            throw new UnsupportedOperationException();
        }
        Map<String, List<Integer>> invertedLists = new TreeMap<>();
        Map<Integer, Document> documents = new HashMap<>();
        Table<String, Integer, List<Integer>> positions = TreeBasedTable.create();
        getSegment(segmentNum, true, invertedLists, documents, positions);
        return new PositionalIndexSegmentForTest(invertedLists, documents, positions);
    }

    /**
     * ================HELPER FUNCTIONS==================
     */

    private void getSegment(int segmentNum, boolean isPositional,
                            Map<String, List<Integer>> invertedLists,
                            Map<Integer, Document> documents, Table<String, Integer, List<Integer>> positions){

        SegmentInDiskManager segMgr = new SegmentInDiskManager(idxFolder, Integer.toString(segmentNum), iiCompressor);
        segMgr.readInitiate();
        // create map(String, List<Integer>) to store keyword and dictionary pair, the list contain 4 attributes
        Map<String, List<Integer>> dictMap = new TreeMap<>();

        // read keyword and dictionary from segment
        while (segMgr.hasKeyWord()) {
            List<Integer> l1 = new ArrayList<>();
            String k1 = segMgr.readKeywordAndDict(l1);
            dictMap.put(k1, l1);
        }

        // initiate for reading posting list
        segMgr.readPostingInitiate();
        segMgr.readTFInitiate();
        if(isPositional) {
            segMgr.readPositionInitiate();
            segMgr.readPositionMetaInitiate();
        }
        // read docId from segment and write to invertedLists
        for (Map.Entry<String, List<Integer>> entry : dictMap.entrySet()) {

            Map<Integer, List<Integer>> docIdList1 = segMgr.readDocIdList(entry.getValue().get(0), entry.getValue().get(1), entry.getValue().get(2), entry.getValue().get(3));

            invertedLists.put(entry.getKey(), docIdList1.keySet().stream().collect(Collectors.toCollection(ArrayList::new)));

            if(isPositional) {
                for (Map.Entry<Integer, List<Integer>> position : docIdList1.entrySet()) {
                    List<Integer> positionList = segMgr.readPosList(position.getValue().get(0),
                            position.getValue().get(1), position.getValue().get(2));
                    positions.put(entry.getKey(), position.getKey(), positionList);
                }
            }
        }

        DocumentStore mapDBGetIdx = MapdbDocStore.createOrOpenReadOnly(idxFolder + "DocStore_" + segmentNum);

        Iterator<Map.Entry<Integer, Document>> it = mapDBGetIdx.iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Document> m = it.next();
            documents.put(m.getKey(), m.getValue());
        }

        mapDBGetIdx.close();
    }
    private void merge(int id1, int id2) {
        // get segment id and docId size
        int sz1;

        DocumentStore mapDB1 = MapdbDocStore.createOrOpen(idxFolder + "DocStore_" + id1);
        sz1 = (int) mapDB1.size();


        DocumentStore mapDB2 = MapdbDocStore.createOrOpen(idxFolder + "DocStore_" + id2);

        DocumentStore mapdbmerged = MapdbDocStore.createOrOpen(idxFolder + "DocStore_merged");
        /*
         * create map to store keyword and dictionary pair, the list either contain 4 attributes or 8 attributes
         * Specification of value at Map : segId(either 0,1) | page | offset | length  , stored at List of integer
         * If the keyword exist in both segments, the list would have two dictionary (8 attribute)
         */
        Map<String, List<Integer>> mergedMap = new TreeMap<>();

        SegmentInDiskManager segMgr1 = new SegmentInDiskManager(idxFolder, Integer.toString(id1), iiCompressor);
        SegmentInDiskManager segMgr2 = new SegmentInDiskManager(idxFolder, Integer.toString(id2), iiCompressor);
        segMgr1.readInitiate();
        segMgr2.readInitiate();


        // read to fill the map
        int totalLengthKeyword = fillTheMap(mergedMap, segMgr1, segMgr2);

        SegmentInDiskManager segMgrMerge = new SegmentInDiskManager(idxFolder, "mergedSegment", iiCompressor);

        // insert to new segment
        insertAtMergedSegment(mergedMap, segMgr1, segMgr2, segMgrMerge, totalLengthKeyword, sz1);


        //write both to a new docstore after deleting the deleted docs then rename docstore


        Iterator<Integer> docId1 = mapDB1.keyIterator();
        int docID = 0;
        while (docId1.hasNext()) {
            docID = docId1.next();
            mapdbmerged.addDocument(docID, mapDB1.getDocument(docID));
        }
        Iterator<Integer> docId2 = mapDB2.keyIterator();
        while (docId2.hasNext()) {
            docID = docId2.next();
            mapdbmerged.addDocument(docID + sz1, mapDB2.getDocument(docID));
        }

        mapdbmerged.close();
        mapDB1.close();
        mapDB2.close();
        // close
        segMgr1.close();
        segMgr2.close();
        segMgrMerge.close();

        deleteAndRename(id1, id2);
    }

    // Specification of value at Map : segId(either 0,1) | page | offset | length  , stored at List of integer
    private int fillTheMap(Map<String, List<Integer>> mergedMap, SegmentInDiskManager segMgr1, SegmentInDiskManager segMgr2) {
        int totalLengthKeyword = 0;

        String k1 = "", k2 = "";
        boolean has1 = false, has2 = false;
        List<Integer> l1 = new ArrayList<>(), l2 = new ArrayList<>();

        while (!k1.isEmpty() || !k2.isEmpty() || segMgr1.hasKeyWord() || segMgr2.hasKeyWord()) {
            if (!has1 && segMgr1.hasKeyWord()) {
                k1 = segMgr1.readKeywordAndDict(l1);
                l1.add(0, 0);
            }
            if (!has2 && segMgr2.hasKeyWord()) {

                k2 = segMgr2.readKeywordAndDict(l2);
                l2.add(0, 1);
            }

            int cmp = k1.compareTo(k2);
            if (cmp == 0) {
                totalLengthKeyword += k1.getBytes().length;
                l1.addAll(l2);
                mergedMap.put(k1, new ArrayList<>(l1));
                l1.clear();
                l2.clear();
                has1 = false;
                has2 = false;
                k1 = "";
                k2 = "";
            } else if (k2.isEmpty() || (cmp < 0 && !k1.isEmpty())) {
                totalLengthKeyword += k1.getBytes().length;
                mergedMap.put(k1, new ArrayList<>(l1));
                l1.clear();
                has1 = false;
                has2 = true;
                k1 = "";
            } else {
                totalLengthKeyword += k2.getBytes().length;

                mergedMap.put(k2, new ArrayList<>(l2));
                l2.clear();
                has1 = true;
                has2 = false;
                k2 = "";
            }
        }

        return totalLengthKeyword;
    }

    private void insertAtMergedSegment(Map<String, List<Integer>> mergedMap, SegmentInDiskManager segMgr1, SegmentInDiskManager segMgr2, SegmentInDiskManager segMgrMerge, int totalLengthKeyword, int sz1) {
        // allocate dictionary bytebuffer
        segMgrMerge.allocateByteBuffer(totalLengthKeyword, mergedMap.size());

        // allocate the position on start point of keyword
        segMgrMerge.allocateKeywordStart(totalLengthKeyword);

        // initiate for reading posting list
        segMgr1.readPostingInitiate();
        segMgr2.readPostingInitiate();

        // p4
        segMgr1.readTFInitiate();
        segMgr2.readTFInitiate();

        if (isPositionalIndex()) {
            segMgr1.readPositionInitiate();
            segMgr2.readPositionInitiate();
            segMgr1.readPositionMetaInitiate();
            segMgr2.readPositionMetaInitiate();
        }

        int[] lst1Sz = new int[1];
        for (Map.Entry<String, List<Integer>> entry : mergedMap.entrySet()) {

            // extract docIdList
            Map<Integer, List<Integer>> docIdList = extractDocList(lst1Sz, entry.getValue(), segMgr1, segMgr2, sz1);

            byte[] encodedPostingList;
            if (isPositionalIndex()) {
                encodedPostingList = iiCompressor.encode(docIdList.keySet().stream().collect(Collectors.toCollection(ArrayList::new)));
            } else {
                encodedPostingList = new NaiveCompressor().encode(docIdList.keySet().stream().collect(Collectors.toCollection(ArrayList::new)));
            }
            // insert segment
            segMgrMerge.insertKeyWord(entry.getKey());
            int docIdLength = encodedPostingList.length;

            segMgrMerge.insertMetaDataSlot(entry.getKey().getBytes().length, docIdLength, docIdList.size());

            segMgrMerge.insertPostingList(encodedPostingList);

            // p4
            int counter = 0;
            for (Map.Entry<Integer, List<Integer>> docId : docIdList.entrySet()) {
                // p4 insert term frequency list
                int sz = docId.getValue().size();
                segMgrMerge.insertTFList(docId.getValue().get(sz-1));
                if (isPositionalIndex()) {
                    List<Integer> positionalList;
                    if (counter < lst1Sz[0]) {
                        positionalList = segMgr1.readPosList(docId.getValue().get(0),
                                docId.getValue().get(1), docId.getValue().get(2));
                    } else {
                        positionalList = segMgr2.readPosList(docId.getValue().get(0),
                                docId.getValue().get(1), docId.getValue().get(2));
                    }

                    byte[] encodedPositionList;
                    encodedPositionList = iiCompressor.encode(positionalList);
                    segMgrMerge.insertPositionList(encodedPositionList, docId.getValue().size());
                }
                counter++;
            }
        }

        segMgrMerge.allocateNumberOfKeyWord(mergedMap.size());


        // append all dictionary byte to new file
        segMgrMerge.appendAllbyte();

        // append last page
        segMgrMerge.appendPage();

    }

    /**
     * Return list of document IDs along with their postingList location
     */
    private Map<Integer, List<Integer>> extractDocList(int[] list1Sz, List<Integer> v, SegmentInDiskManager segMgr1, SegmentInDiskManager segMgr2, int sz1) {
        Map<Integer, List<Integer>> docIdList1 = new TreeMap<>(), docIdList2 = new TreeMap<>();
        // the keyword exist in both segments
        if (v.size() == 12) {
            docIdList1 = segMgr1.readDocIdList(v.get(1), v.get(2), v.get(3), v.get(4));
            //docIdList2 = segMgr2.readDocIdList(v.get(6), v.get(7), v.get(8), v.get(9));
            docIdList2 = segMgr2.readDocIdList(v.get(7), v.get(8), v.get(9), v.get(10));

        } else {
            // exist in either  1st/2nd segment
            if (v.get(0) == 0) {
                docIdList1 = segMgr1.readDocIdList(v.get(1), v.get(2), v.get(3), v.get(4));
            } else {
                docIdList2 = segMgr2.readDocIdList(v.get(1), v.get(2), v.get(3), v.get(4));
            }
        }

        list1Sz[0] = docIdList1.keySet().size();
        // convert docId in segment 2
        for (Map.Entry<Integer, List<Integer>> entry : docIdList2.entrySet()) {
            docIdList1.put(entry.getKey() + sz1, entry.getValue());
        }

        // concat docId2 to docId1
        return docIdList1;
    }

    private void deleteAndRename(int id1, int id2) {
        // delete segment
        File f1, f2;
        f1 = new File(idxFolder + "segment_" + id1);
        f2 = new File(idxFolder + "segment_" + id2);
        f1.delete();
        f2.delete();

        f1 = new File(idxFolder + "posting_" + id1);
        f2 = new File(idxFolder + "posting_" + id2);
        f1.delete();
        f2.delete();

        f1 = new File(idxFolder + "position_" + id1);
        f2 = new File(idxFolder + "position_" + id2);
        f1.delete();
        f2.delete();

        f1 = new File(idxFolder + "meta_" + id1);
        f2 = new File(idxFolder + "meta_" + id2);
        f1.delete();
        f2.delete();


        f1 = new File(idxFolder + "tf_" + id1);
        f2 = new File(idxFolder + "tf_" + id2);
        f1.delete();
        f2.delete();

        // rename segment
        f1 = new File(idxFolder + "segment_mergedSegment");
        f2 = new File(idxFolder + "segment_" + id1 / 2);
        boolean success = f1.renameTo(f2);

        if (!success) throw new UnsupportedOperationException("rename segment fail");

        f1 = new File(idxFolder + "posting_mergedSegment");
        f2 = new File(idxFolder + "posting_" + id1 / 2);
        success = f1.renameTo(f2);

        if (!success) throw new UnsupportedOperationException("rename segment fail");

        f1 = new File(idxFolder + "position_mergedSegment");
        f2 = new File(idxFolder + "position_" + id1 / 2);
        f1.renameTo(f2);

        f1 = new File(idxFolder + "meta_mergedSegment");
        f2 = new File(idxFolder + "meta_" + id1 / 2);
        success = f1.renameTo(f2);

        f1 = new File(idxFolder + "tf_mergedSegment");
        f2 = new File(idxFolder + "tf_" + id1 / 2);
        success = f1.renameTo(f2);

        // delete 2nd document store
        f2 = new File(idxFolder + "DocStore_" + id2);
        f2.delete();

        //delete 1st document store
        f1 = new File(idxFolder + "DocStore_" + id1);
        f1.delete();


        // rename merged document store
        f1 = new File(idxFolder + "DocStore_merged");
        f2 = new File(idxFolder + "DocStore_" + id1 / 2);
        success = f1.renameTo(f2);

        if (!success) throw new UnsupportedOperationException("rename docstore fail");
    }

    private void reset() {
        mapDB.close();
        ++NUM_SEQ;
        keyWordMap.clear();
        document_Counter = 0;
        totalLengthKeyword = 0;
    }

    private File[] getFiles(String fileName) {
        File dir = new File(idxFolder);
        File[] files = dir.listFiles((d, name) -> name.startsWith(fileName));
        return files;

    }

    private Iterator<Document> searchKewords(List<String> keywords, Enum searchOperation) {
        Iterator<Document> iterator = new ArrayList<Document>().iterator();
        File[] files = getFiles("segment");
        sort(files);
        for (int i = 0; i < files.length; ++i) {
            Set<Integer> postingListset = new TreeSet<>();
            for (int j = 0; j < keywords.size(); j++) {
                Map<Integer, List<Integer>> postingList = searchSegment(files[i].getName().substring(8), keywords.get(j));//.keySet().stream().collect(Collectors.toCollection(ArrayList::new));
                if (searchOperation == SearchOperation.AND_SEARCH && j > 0) {
                    postingListset.retainAll(postingList.keySet());
                } else {
                    postingListset.addAll(postingList.keySet());
                }
            }
            if (postingListset.size() >= 1) {
                Collections.sort(Lists.newArrayList(postingListset));
                DocumentStore mapDBSearch = MapdbDocStore.createOrOpenReadOnly(idxFolder + "DocStore_" + i);
                iterator = Iterators.concat(iterator, Iterators.transform(postingListset.iterator(), entry -> mapDBSearch.getDocument(entry)));
            }
        }
        return iterator;
    }

    private Map<Integer, List<Integer>> searchSegment(String segment, String keyword) {
        Map<Integer, List<Integer>> postingList = new TreeMap<>();
        SegmentInDiskManager segMgr = new SegmentInDiskManager(idxFolder, segment, iiCompressor);
        segMgr.readInitiate();
        Map<String, List<Integer>> dictMap = new TreeMap<>();
        while (segMgr.hasKeyWord()) {
            List<Integer> l1 = new ArrayList<>();
            String k1 = segMgr.readKeywordAndDict(l1);
            dictMap.put(k1, l1);
        }

        segMgr.readPostingInitiate();
        if (isPositionalIndex()) {
            segMgr.readPositionMetaInitiate();
            segMgr.readPositionInitiate();

        }
        if (dictMap.containsKey(keyword)) {
            postingList = segMgr.readDocIdList(dictMap.get(keyword).get(0),
                    dictMap.get(keyword).get(1), dictMap.get(keyword).get(2), dictMap.get(keyword).get(3));
        }
        segMgr.close();
        return postingList;
    }


    private void sort(File[] files) {
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                int n1 = extractNumber(o1.getName());
                int n2 = extractNumber(o2.getName());
                return n1 - n2;
            }

            private int extractNumber(String name) {
                int i = 0;
                try {
                    int s = name.indexOf('_') + 1;
                    String number = name.substring(s);
                    i = Integer.parseInt(number);
                } catch (Exception e) {
                    i = 0;
                }
                return i;
            }
        });
    }

    private boolean isPositionalIndex() {
        return iiCompressor != null;
    }
}
