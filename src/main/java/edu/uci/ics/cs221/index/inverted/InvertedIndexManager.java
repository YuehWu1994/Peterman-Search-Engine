package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;
import edu.uci.ics.cs221.storage.Document;
import org.eclipse.collections.impl.tuple.ImmutableEntry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.ArrayList;
import java.io.File;

import static com.google.common.collect.Maps.immutableEntry;

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
    private static int DEFAULT_ADD_DOCUMENT_THRESHOLD = 50;

    /**
     * Map keyword with list of document ID
     */
    private static Map<String, Set<Integer>> keyWordMap;

    private static Map<Integer, Document> documentsMap;

    private static DocumentStore mapDB;

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

    private enum SearchOperation {
        AND_SEARCH,
        OR_SEARCH
    }

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        document_Counter = 0;
    }

    /**
     * Creates an inverted index manager with the folder and an analyzer
     */
    public static InvertedIndexManager createOrOpen(String indexFolder, Analyzer analyzer) {

        try {

            idxFolder = indexFolder + "/";
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
        documentsMap.put(document_Counter, document);

        ++document_Counter;
        if(document_Counter == DEFAULT_ADD_DOCUMENT_THRESHOLD)
        {
            mapDB = MapdbDocStore.createWithBulkLoad(idxFolder + "Doc_Store" + NUM_SEQ, Iterators.transform(documentsMap.entrySet().iterator(), entry -> immutableEntry(entry.getKey(), entry.getValue())));
            documentsMap.clear();
            mapDB.close();
        }
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
        if (document_Counter == 0) {
            return;
        }

        SegmentInDiskManager segMgr = new SegmentInDiskManager(idxFolder, Integer.toString(NUM_SEQ));


        // allocate dictionary bytebuffer
        segMgr.allocateByteBuffer(totalLengthKeyword, keyWordMap.size());


        // allocate the position on start point of keyword
        segMgr.allocateKeywordStart(totalLengthKeyword);

        // insert keyword, metadata, docID respectively
        //System.out.println("##### Start to insert keyword #####");

        for (String keyword : keyWordMap.keySet()) {
            segMgr.insertKeyWord(keyword);
        }

        // allocate the number of keyword on start point of dictionary
        segMgr.allocateNumberOfKeyWord(keyWordMap.size());

        //System.out.println("##### Start to insert dictionary slot #####");
        for (Map.Entry<String, Set<Integer>> entry : keyWordMap.entrySet()) {
            segMgr.insertMetaDataSlot(entry.getKey().getBytes().length, entry.getValue().size() * Integer.BYTES);
        }


        // append all dictionary byte to new file
        segMgr.appendAllbyte();



        System.out.println("##### Start to insert docID #####");
        for (Map.Entry<String, Set<Integer>> entry : keyWordMap.entrySet()) {
            //System.out.println("Insert docID of " + entry.getKey());
            segMgr.insertListOfDocID(entry.getValue());
        }

        segMgr.appendPage();

        segMgr.close();

        reset();
        //System.out.println("\n\n");
    }


    /**
     * Merges all the disk segments of the inverted index pair-wise.
     */
    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);

        String seg1 = "";

        File[] files = getFiles("segment");
        File[] files_poisting = getFiles("posting");

        Arrays.sort(files);
        Arrays.sort(files_poisting);

        for (int i = 0; i < files.length; ++i) {
            if (seg1 != "") {
                // merge
                merge(Integer.parseInt(seg1.substring(7)), Integer.parseInt(files[i].getName().substring(7)));

                // after merge
                seg1 = "";
            } else seg1 = files[i].getName();
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
     * Iterates through all the documents in all disk segments.
     */
    public Iterator<Document> documentIterator() {
        Iterator<Document> iterator = new ArrayList<Document>().iterator();

        File[] files = getFiles("Doc_Store");
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
        Map<String, List<Integer>> invertedLists = new TreeMap<>();
        Map<Integer, Document> documents = new HashMap<>();


        if (!Files.exists(Paths.get(idxFolder + "segment" + segmentNum))) {
            return null;
        }

        // ##### invertedLists  #####
        SegmentInDiskManager segMgr = new SegmentInDiskManager(idxFolder, Integer.toString(segmentNum));
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

        // read docId from segment and write to invertedLists
        for (Map.Entry<String, List<Integer>> entry : dictMap.entrySet()) {
            List<Integer> v = entry.getValue();

            List<Integer> docIdList1 = segMgr.readDocIdList(v.get(0), v.get(1), v.get(2));

            invertedLists.put(entry.getKey(), docIdList1);

        }

        // documents
        DocumentStore mapDBGetIdx = MapdbDocStore.createOrOpenReadOnly(idxFolder + "Doc_Store" + segmentNum);
        Iterator<Map.Entry<Integer, Document>> it = mapDBGetIdx.iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Document> m = it.next();
            documents.put(m.getKey(), m.getValue());
        }

        mapDBGetIdx.close();
        return new InvertedIndexSegmentForTest(invertedLists, documents);
    }

    /**
     * ================HELPER FUNCTIONS==================
     */
    private void merge(int id1, int id2) {
        // get segment id and docId size
        int sz1, sz2;

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

        SegmentInDiskManager segMgr1 = new SegmentInDiskManager(idxFolder, Integer.toString(id1));
        SegmentInDiskManager segMgr2 = new SegmentInDiskManager(idxFolder, Integer.toString(id2));
        segMgr1.readInitiate();
        segMgr2.readInitiate();


        // read to fill the map
        int totalLengthKeyword = fillTheMap(mergedMap, segMgr1, segMgr2);

        SegmentInDiskManager segMgrMerge = new SegmentInDiskManager(idxFolder, "mergedSegment");

        // insert to new segment
        insertAtMergedSegment(mergedMap, segMgr1, segMgr2, segMgrMerge, totalLengthKeyword, sz1);


        // append last page
        segMgrMerge.appendPage();

        // add documentStore at doc1 toward doc0
        for (int i = 0; i < sz2; ++i) {
            Document d = mapDB2.getDocument(i);
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

        // append all dictionary byte to new file
        segMgrMerge.appendAllbyte();

        // initiate for reading posting list
        segMgr1.readPostingInitiate();
        segMgr2.readPostingInitiate();


        // read docId from segment 1, 2 and write to new segment
        for (Map.Entry<String, List<Integer>> entry : mergedMap.entrySet()) {
            List<Integer> docIdList1 = new ArrayList<>(), docIdList2 = new ArrayList<>();

            List<Integer> v = entry.getValue();

            // the keyword exist in both segments
            if (v.size() == 8) {
                docIdList1 = segMgr1.readDocIdList(v.get(1), v.get(2), v.get(3));
                docIdList2 = segMgr2.readDocIdList(v.get(5), v.get(6), v.get(7));
            } else {
                // exist in either  1st/2nd segment
                if (v.get(0) == 0) docIdList1 = segMgr1.readDocIdList(v.get(1), v.get(2), v.get(3));
                else docIdList2 = segMgr2.readDocIdList(v.get(1), v.get(2), v.get(3));
            }

            // convert docId in segment 2

            for (int i = 0; i < docIdList2.size(); ++i) {
                int id_v = docIdList2.get(i);
                id_v += sz1;
                docIdList2.set(i, id_v);
            }

            // concat docId2 to docId1
            docIdList1.addAll(docIdList2);

            segMgrMerge.insertListOfDocID(new HashSet<>(docIdList1));
        }

    }

    private void deleteAndRename(int id1, int id2) {
        // delete segment
        File f1, f2;
        f1 = new File(idxFolder + "segment" + id1);
        f2 = new File(idxFolder + "segment" + id2);
        f1.delete();
        f2.delete();

        f1 = new File(idxFolder + "posting" + id1);
        f2 = new File(idxFolder + "posting" + id2);
        f1.delete();
        f2.delete();

        // rename segment
        f1 = new File(idxFolder + "segmentmergedSegment");
        f2 = new File(idxFolder + "segment" + id1 / 2);
        boolean success = f1.renameTo(f2);

        if (!success) throw new UnsupportedOperationException("rename segment fail");

        f1 = new File(idxFolder + "postingmergedSegment");
        f2 = new File(idxFolder + "posting" + id1 / 2);
        success = f1.renameTo(f2);

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

    private void reset() {

        //mapDB.close();
        ++NUM_SEQ;
        keyWordMap.clear();
        document_Counter = 0;
        totalLengthKeyword = 0;
        documentsMap.clear();
    }

    private File[] getFiles(String fileName) {
        File dir = new File(idxFolder);
        File[] files = dir.listFiles((d, name) -> name.startsWith(fileName));
        return files;

    }

    private Iterator<Document> searchKewords(List<String> keywords, Enum searchOperation) {
        Iterator<Document> iterator = new ArrayList<Document>().iterator();
        File[] files = getFiles("segment");
        Arrays.sort(files);
        for (int i = 0; i < files.length; ++i) {
            //SegmentInDiskManager segMgr = new SegmentInDiskManager(Paths.get(files[i].getPath()));
            SegmentInDiskManager segMgr = new SegmentInDiskManager(idxFolder, files[i].getPath().substring(7));
            segMgr.readInitiate();
            Map<String, List<Integer>> dictMap = new TreeMap<>();
            Set<Integer> postingListset = new LinkedHashSet<>();
            while (segMgr.hasKeyWord()) {
                List<Integer> l1 = new ArrayList<>();
                String k1 = segMgr.readKeywordAndDict(l1);
                dictMap.put(k1, l1);
            }

            // Maybe here (Sadeem)
            // segMgr.readPostingInitiate()

            for (int j = 0; j < keywords.size(); j++) {
                int pos = Collections.binarySearch(Lists.newArrayList(dictMap.keySet()), keywords.get(j));
                if (pos >= 0) {
                    List<Integer> postingList = segMgr.readDocIdList(dictMap.get(keywords.get(j)).get(0),
                            dictMap.get(keywords.get(j)).get(1), dictMap.get(keywords.get(j)).get(2));
                    if (searchOperation == SearchOperation.AND_SEARCH && j > 0) {
                        postingListset.retainAll(postingList);
                    } else {
                        postingListset.addAll(postingList);
                    }
                }
            }
            if (postingListset.size() >= 1) {
                DocumentStore mapDBSearch = MapdbDocStore.createOrOpen(idxFolder + "Doc_Store" + i);
                Iterators.removeIf(mapDBSearch.iterator(), entry -> !postingListset.contains(entry.getKey()));
                iterator = Iterators.concat(iterator, Iterators.transform(mapDBSearch.iterator(), entry -> entry.getValue()));
                mapDBSearch.close();
            }
        }
        return iterator;
    }
}
