package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
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
import java.util.*;
import java.util.ArrayList;
import java.io.File;


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
    private static Map<String, Set<Integer>> keyWordMap;


    /**
     * Number of sequence in disk (for merge)
     */
    private static int NUM_SEQ;

    /**
     * Document Counter (for flush)
     */
    private static Integer document_Counter;


    /**
     * Total length of keyword (in order to build dictionary on page file)
     */
    private static Integer totalLengthKeyword;


    private static String idxFolder;

    private static Analyzer iiAnalyzer;

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        document_Counter = 0;
    }

    /**
     * Creates an inverted index manager with the folder and an analyzer
     */
    public static InvertedIndexManager createOrOpen(String indexFolder, Analyzer analyzer) {

        try {
            idxFolder = indexFolder+"/";
            NUM_SEQ = 0;
            document_Counter = 0;
            totalLengthKeyword = 0;
            keyWordMap = new TreeMap<>();
            iiAnalyzer = analyzer;

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
     *
     * @param document
     */
    public void addDocument(Document document) {

        // process (analyzer) text in the document

        List<String> word = iiAnalyzer.analyze(document.getText());


        // record on hashmap
        for (String w : word) {
            if (!keyWordMap.containsKey(w)) {
                keyWordMap.put(w, new HashSet<>());
                totalLengthKeyword += w.getBytes().length;
            }

            keyWordMap.get(w).add(document_Counter);
        }

        // add document into DocStore
        DocumentStore mapDB = MapdbDocStore.createOrOpen(idxFolder + "Doc_Store" + NUM_SEQ); // not sure
        mapDB.addDocument(document_Counter, document);
        mapDB.close();

        ++document_Counter;

        if (document_Counter == DEFAULT_FLUSH_THRESHOLD) {
            flush();
        }

        if (NUM_SEQ == DEFAULT_MERGE_THRESHOLD) {
            mergeAllSegments();
        }
    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {
        if (document_Counter == 0)
        {
            return;
        }

        SegmentInDiskManager segMgr = new SegmentInDiskManager(Paths.get(idxFolder + "segment" + NUM_SEQ));

        // allocate the position on start point of keyword
        segMgr.allocateKeywordStart(totalLengthKeyword);

        // insert keyword, metadata, docID respectively
        System.out.println("##### Start to insert keyword #####");

        // @@@@@
        for (String keyword : keyWordMap.keySet()) {
            segMgr.insertKeyWord(keyword);
        }

        // allocate the number of keyword on start point of dictionary
        segMgr.allocateNumberOfKeyWord(keyWordMap.size());

        System.out.println("##### Start to insert dictionary slot #####");
        //int keyWordNum = 0;
        for (Map.Entry<String, Set<Integer>> entry : keyWordMap.entrySet()) {
            segMgr.insertMetaDataSlot(entry.getKey().getBytes().length, entry.getValue().size() * Integer.BYTES);//keyWordNum, keyWordMap.size(), entry.getKey().getBytes().length, entry.getValue());//.size() * Integer.BYTES);
            //keyWordNum++;
        }

        System.out.println("##### Start to insert docID #####");
        for (Map.Entry<String, Set<Integer>> entry : keyWordMap.entrySet()) {
            System.out.println("Insert docID of " + entry.getKey());
            segMgr.insertListOfDocID(entry.getValue());
        }

        // @@@@@ append the last page before I close (if not append)
        segMgr.appendPage();

        segMgr.close();

        reset();
        System.out.println("\n\n");
    }

    public void reset() {
        ++NUM_SEQ;
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
        File p = new File(idxFolder);

        List<String> entries = Arrays.asList(p.list());
        Collections.sort(entries);

        for (int i = 0; i < entries.size(); ++i) {
            if (entries.get(i).length() > 7 && entries.get(i).substring(0, 7).equals("segment")) {
                if (seg1 != "") {
                    // merge
                    merge(seg1, entries.get(i));

                    // after merge
                    seg1 = "";
                } else seg1 = entries.get(i);
            }
        }

        // minus NUM_SEQ by half
        NUM_SEQ = NUM_SEQ / 2;
    }

    public void merge(String seg1, String seg2) {
        // get segment id and docId size
        int sz1, sz2;
        int id1 = Integer.parseInt(seg1.substring(7));
        int id2 = Integer.parseInt(seg2.substring(7));


        DocumentStore mapDB1 = MapdbDocStore.createOrOpen(idxFolder + "Doc_Store" + id1);
        sz1 = (int) mapDB1.size();


        DocumentStore mapDB2 = MapdbDocStore.createOrOpen(idxFolder + "Doc_Store" + id2);
        sz2 = (int) mapDB2.size();


        /*
         * create map to store keyword and dictionary pair, the list either contain 4 attributes or 8 attributes
         * Specification of value at Map : segId(either 0,1) | page | offset | length  , stored at List of integer
         * If the keyword exist in both segments, the list would have two dictionary (8 attribute)
         */
        Map<String, List<Integer>> mergedMap = new TreeMap<>();

        SegmentInDiskManager segMgr1 = new SegmentInDiskManager(Paths.get(idxFolder + "segment" + id1));
        SegmentInDiskManager segMgr2 = new SegmentInDiskManager(Paths.get(idxFolder + "segment" + id2));
        segMgr1.readInitiate();
        segMgr2.readInitiate();


        // read to fill the map
        int totalLengthKeyword = fillTheMap(mergedMap, segMgr1, segMgr2);

        SegmentInDiskManager segMgrMerge = new SegmentInDiskManager(Paths.get(idxFolder + "mergedSegment"));

        // insert to new segment
        insertAtMergedSegment(mergedMap, segMgr1, segMgr2, segMgrMerge, totalLengthKeyword, sz1);

        // add documentStore at doc1 toward doc0
        for (int i = 0; i < sz2; ++i) {
            Document d = mapDB2.getDocument(0);
            mapDB1.addDocument(i + sz1, d);
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
    public int fillTheMap(Map<String, List<Integer>> mergedMap, SegmentInDiskManager segMgr1, SegmentInDiskManager segMgr2) {
        int totalLengthKeyword = 0;

        String k1 = "", k2 = "";
        boolean has1 = false, has2 = false;
        List<Integer> l1 = new ArrayList<>(), l2 = new ArrayList<>();
        while (segMgr1.hasKeyWord() || segMgr2.hasKeyWord()) {


            if (!has1 && segMgr1.hasKeyWord()) {
                l1.clear();
                k1 = segMgr1.readKeywordAndDict(l1);
                l1.add(0, 0);
            }
            if (!has2 && segMgr2.hasKeyWord()) {
                l2.clear();
                k2 = segMgr2.readKeywordAndDict(l2);
                l2.add(0, 1);
            }

            int cmp = k1.compareTo(k2);
            if (cmp == 0) {
                totalLengthKeyword += k1.getBytes().length;
                l1.addAll(l2);
                mergedMap.put(k1, l1);
                has1 = false;
                has2 = false;
                k1 = "";
                k2 = "";
            } else if (k2.isEmpty() || (cmp < 0 && !k1.isEmpty())) {
                totalLengthKeyword += k1.getBytes().length;
                mergedMap.put(k1, l1);
                has1 = false;
                has2 = true;
                k1 = "";
            } else {
                totalLengthKeyword += k2.getBytes().length;
                mergedMap.put(k2, l2);
                has1 = true;
                has2 = false;
                k2 = "";
            }
        }
        return totalLengthKeyword;
    }


    public void insertAtMergedSegment(Map<String, List<Integer>> mergedMap, SegmentInDiskManager segMgr1, SegmentInDiskManager segMgr2, SegmentInDiskManager segMgrMerge, int totalLengthKeyword, int sz1) {
        // allocate the position on start point of keyword
        segMgrMerge.allocateKeywordStart(totalLengthKeyword);


        // insert keyword, metadata, docID respectively
        for (String keyword : mergedMap.keySet()) {
            segMgrMerge.insertKeyWord(keyword);
        }


        // allocate the number of keyword on start point of dictionary
        segMgrMerge.allocateNumberOfKeyWord(mergedMap.size());

        for (Map.Entry<String, List<Integer>> entry : mergedMap.entrySet()) {
            int docIdLength = entry.getValue().get(3);

            // if this key word exists in both segments
            if (entry.getValue().size() == 8) docIdLength += entry.getValue().get(7);

            segMgrMerge.insertMetaDataSlot(entry.getKey().getBytes().length, docIdLength);
        }


        // read docId from segment 1, 2 and write to new segment
        for (Map.Entry<String, List<Integer>> entry : mergedMap.entrySet()) {
            List<Integer> docIdList1 = new ArrayList<>(), docIdList2 = new ArrayList<>();

            List<Integer> v = entry.getValue();

            // the keyword exist in both segments
            if (v.size() == 8) {
                docIdList1 = segMgr1.readDocIdList(v.get(3));
                docIdList2 = segMgr2.readDocIdList(v.get(7));
            } else {
                // exist in either  1st/2nd segment
                if (v.get(0) == 0) docIdList1 = segMgr1.readDocIdList(v.get(3));
                else docIdList2 = segMgr2.readDocIdList(v.get(3));
            }

            // convert docId in segment 2
            for (int i : docIdList2) i += sz1;

            // concat docId2 to docId1
            docIdList1.addAll(docIdList2);

            segMgrMerge.insertListOfDocID(new HashSet<>(docIdList1));
        }


        //segMgrMerge.allocateDocIDEnd();
    }

    public void deleteAndRename(int id1, int id2) {
        // delete segment
        File f1, f2;
        f1 = new File(idxFolder + "segment" + id1);
        f2 = new File(idxFolder + "segment" + id2);
        f1.delete();
        f2.delete();

        // rename segment
        f1 = new File(idxFolder + "mergedSegment");
        f2 = new File(idxFolder + "segment" + id1 / 2);
        boolean success = f1.renameTo(f2);

        if (!success) throw new UnsupportedOperationException("rename segment fail");


        // delete 2nd document store
        f2 = new File(idxFolder + "Doc_Store" + id2);
        f2.delete();

        // rename 1st document store
        f1 = new File(idxFolder + "Doc_Store" + id1);
        f2 = new File(idxFolder + "Doc_Store" + id1 / 2);
        success = f1.renameTo(f2);

        if (!success) throw new UnsupportedOperationException("rename docstore fail");
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
        Iterator<Document> iterator = new ArrayList<Document>().iterator();

        File dir = new File(idxFolder);
        File[] files = dir.listFiles((d, name) -> name.startsWith("Doc_Store"));
        for (int i = 0; i < files.length; ++i) {
                DocumentStore mapDB = MapdbDocStore.createOrOpen(  files[i].getPath());
                iterator = Iterators.concat(iterator, Iterators.transform(mapDB.iterator(), entry -> entry.getValue()));
                mapDB.close();
        }
        return iterator;
    }

    /**
     * Deletes all documents in all disk segments of the inverted index that match the query.
     *
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
     * <p>
     * Q: used in disk or in-memory
     */
    public int getNumSegments() {
        File dir = new File(idxFolder);
        File[] files = dir.listFiles((d, name) -> name.startsWith("segment"));
        return files.length;
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
        Map<String, List<Integer>> invertedLists = new TreeMap<>();
        Map<Integer, Document> documents = new HashMap<>();

        if(! Files.exists(Paths.get(idxFolder + "segment" + segmentNum)))
        {
            return null;
        }
        // ##### invertedLists  #####
        SegmentInDiskManager segMgr = new SegmentInDiskManager(Paths.get(idxFolder + "segment" + segmentNum));
        segMgr.readInitiate();

        // create map(String, List<Integer>) to store keyword and dictionary pair, the list contain 4 attributes
        Map<String, List<Integer>> dictMap = new TreeMap<>();

        // read keyword and dictionary from segment
        while (segMgr.hasKeyWord()) {
            List<Integer> l1 = new ArrayList<>();
            String k1 = segMgr.readKeywordAndDict(l1);

            l1.add(0, 0);
            dictMap.put(k1, l1);

        }

        // read docId from segment and write to invertedLists
        for (Map.Entry<String, List<Integer>> entry : dictMap.entrySet()) {
            List<Integer> v = entry.getValue();

            List<Integer> docIdList1 = segMgr.readDocIdList(v.get(3));

            invertedLists.put(entry.getKey(), docIdList1);

        }

        // documents
        DocumentStore mapDB = MapdbDocStore.createOrOpen(idxFolder + "Doc_Store" + segmentNum);
        Iterator<Map.Entry<Integer, Document>> it = mapDB.iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Document> m = it.next();
            documents.put(m.getKey(), m.getValue());
        }

        mapDB.close();
        return new InvertedIndexSegmentForTest(invertedLists, documents);
    }


}
