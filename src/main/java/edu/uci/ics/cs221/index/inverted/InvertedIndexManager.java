package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.sun.tools.javac.util.ArrayUtils;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;
import edu.uci.ics.cs221.storage.Document;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
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
            //i think we shouldn't open it in constructor
            //because what if we actually never flush. and we exit the program because of some other error
            //professor said assume before flushing all documents can be kept in memory
            //however when you merge you can't keep all docs in memory

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

        File f = new File(idxFolder + "DocStore_" + NUM_SEQ);
        if (!f.exists()) {
            mapDB = MapdbDocStore.createOrOpen(idxFolder + "DocStore_" + NUM_SEQ);
        }
        else
        {
            mapDB = MapdbDocStore.createOrOpen(idxFolder + "DocStore_" + NUM_SEQ);
            document_Counter = (int)mapDB.size();
        }

        mapDB.addDocument(document_Counter, document);


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
        if (document_Counter == 0) {
            return;
        }

        SegmentInDiskManager segMgr = new SegmentInDiskManager(idxFolder, Integer.toString(NUM_SEQ));


        // allocate dictionary bytebuffer
        segMgr.allocateByteBuffer(totalLengthKeyword, keyWordMap.size());


        // allocate the position on start point of keyword
        segMgr.allocateKeywordStart(totalLengthKeyword);


        // insert keyword, metadata, docID in one pass
        for (Map.Entry<String, Set<Integer>> entry : keyWordMap.entrySet()) {
            segMgr.insertKeyWord(entry.getKey());
            segMgr.insertMetaDataSlot(entry.getKey().getBytes().length, entry.getValue().size() * Integer.BYTES);
            segMgr.insertListOfDocID(entry.getValue());
        }

        // allocate the number of keyword on start point of dictionary
        segMgr.allocateNumberOfKeyWord(keyWordMap.size());


        // append all dictionary byte to new file
        segMgr.appendAllbyte();
        segMgr.appendPage();

        segMgr.close();

        reset();
    }


    /**
     * Merges all the disk segments of the inverted index pair-wise.
     */
    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);

        String seg1 = "";
        File file;
        int[] deletedDocs1 = null;
        int[] deletedDocs2 = null;
        File[] files = getFiles("segment");
        File[] files_poisting = getFiles("posting");
        List<File> deleteFiles = Arrays.asList(getFiles("deleted"));
        int numOfDocs;

        sort(files);
        sort(files_poisting);

        for (int i = 0; i < files.length; ++i) {
            String fileName = files[i].getName().replace("segment", "deleted");
            if (seg1 != "") {
                if (containsFile(deleteFiles, fileName)) {
                    deletedDocs2 = getDeletedDocsList(i);
                    numOfDocs = getNumOfDocs(i);
                    if (numOfDocs == deletedDocs2.length) {
                        file = new File(idxFolder + "DocStore_" + i);
                        file.delete();
                        file = new File(idxFolder + "posting_" + i);
                        file.delete();
                        file = new File(idxFolder + "segment_" + i);
                        file.delete();
                        file = new File(getDeletedFile(i).getPath());
                        file.delete();
                        continue;
                    }
                }
                // merge
                merge(Integer.parseInt(seg1.substring(8)), Integer.parseInt(files[i].getName().substring(8)), deletedDocs1, deletedDocs2);

                // after merge
                seg1 = "";
            } else {
                if (containsFile(deleteFiles, fileName)) {
                    deletedDocs1 = getDeletedDocsList(i);
                    numOfDocs = getNumOfDocs(i);
                    if (numOfDocs == deletedDocs1.length) {
                        file = new File(idxFolder + "DocStore_" + i);
                        file.delete();
                        file = new File(idxFolder + "posting_" + i);
                        file.delete();
                        file = new File(idxFolder + "segment_" + i);
                        file.delete();
                        file = new File(getDeletedFile(i).getPath());
                        file.delete();
                        continue;
                    }
                }
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

        File[] files = getFiles("segment");
        sort(files);
        if (!keyword.equals("")) {
            List<String> keywords = iiAnalyzer.analyze(keyword);
            for (int i = 0; i < files.length; ++i) {
                List<Integer> postingList = searchSegment(files[i].getName().substring(8), keywords.get(0));
                if (!postingList.isEmpty()) {
                    //name deleted file delete_segment#_number of documents to be deleted
                    //if 
                    Path pfcPath = Paths.get(idxFolder + "deleted_" + i + "-" + postingList.size());
                    PageFileChannel pfc = PageFileChannel.createOrOpen(pfcPath);
                    ByteBuffer bb = ByteBuffer.allocate(postingList.size() * Integer.BYTES);
                    for (int j = 0; j < postingList.size(); j++) {
                        bb.putInt(postingList.get(j));
                    }
                    pfc.appendAllBytes(bb);
                    pfc.close();
                }
            }
        }
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


        if (!Files.exists(Paths.get(idxFolder + "segment_" + segmentNum))) {
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

        DocumentStore mapDBGetIdx = MapdbDocStore.createOrOpenReadOnly(idxFolder + "DocStore_" + segmentNum);

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
    private void merge(int id1, int id2, int[] deletedDocs1, int[] deletedDocs2) {
        // get segment id and docId size
        int sz1, sz2;

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

        SegmentInDiskManager segMgr1 = new SegmentInDiskManager(idxFolder, Integer.toString(id1));
        SegmentInDiskManager segMgr2 = new SegmentInDiskManager(idxFolder, Integer.toString(id2));
        segMgr1.readInitiate();
        segMgr2.readInitiate();


        // read to fill the map
        int totalLengthKeyword = fillTheMap(mergedMap, segMgr1, segMgr2);

        SegmentInDiskManager segMgrMerge = new SegmentInDiskManager(idxFolder, "mergedSegment");

        // insert to new segment
        insertAtMergedSegment(mergedMap, segMgr1, segMgr2, segMgrMerge, totalLengthKeyword, sz1, deletedDocs1, deletedDocs2);


        //write both to a new docstore after deleting the deleted docs then rename docstore


        Iterator<Integer> docId1 = mapDB1.keyIterator();
        int docID = 0;
        while(docId1.hasNext())
        {
            docID = docId1.next();
            if(deletedDocs1 != null && contains(deletedDocs1, docID))
            {
                continue;
            }
            mapdbmerged.addDocument(docID, mapDB1.getDocument(docID));
        }
        Iterator<Integer> docId2 = mapDB2.keyIterator();
        while(docId2.hasNext())
        {
            docID = docId2.next();
            if(deletedDocs2 != null && contains(deletedDocs2, docID))
            {
                continue;
            }
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

    private void insertAtMergedSegment(Map<String, List<Integer>> mergedMap, SegmentInDiskManager segMgr1, SegmentInDiskManager segMgr2, SegmentInDiskManager segMgrMerge, int totalLengthKeyword, int sz1, int[] deletedDocs1, int[] deletedDocs2) {
        // allocate dictionary bytebuffer
        segMgrMerge.allocateByteBuffer(totalLengthKeyword, mergedMap.size());

        // allocate the position on start point of keyword
        segMgrMerge.allocateKeywordStart(totalLengthKeyword);

        // initiate for reading posting list
        segMgr1.readPostingInitiate();
        segMgr2.readPostingInitiate();


        for (Map.Entry<String, List<Integer>> entry : mergedMap.entrySet()) {

            // extract docIdList
            List<Integer> docIdList = extractDocList(entry.getValue(), segMgr1, segMgr2, sz1, deletedDocs1, deletedDocs2);


            // insert segment
            segMgrMerge.insertKeyWord(entry.getKey());
            int docIdLength = entry.getValue().get(3);
            // if this key word exists in both segments
            if (entry.getValue().size() == 8) docIdLength += entry.getValue().get(7);

            segMgrMerge.insertMetaDataSlot(entry.getKey().getBytes().length, docIdLength);
            segMgrMerge.insertListOfDocID(new HashSet<>(docIdList));
        }

        segMgrMerge.allocateNumberOfKeyWord(mergedMap.size());


        // append all dictionary byte to new file
        segMgrMerge.appendAllbyte();

        // append last page
        segMgrMerge.appendPage();

    }

    private List<Integer> extractDocList(List<Integer> v, SegmentInDiskManager segMgr1, SegmentInDiskManager segMgr2, int sz1, int[] deletedDocs1, int[] deletedDocs2) {
        List<Integer> docIdList1 = new ArrayList<>(), docIdList2 = new ArrayList<>();


        // the keyword exist in both segments
        if (v.size() == 8) {
            docIdList1 = segMgr1.readDocIdList(v.get(1), v.get(2), v.get(3));
            docIdList2 = segMgr2.readDocIdList(v.get(5), v.get(6), v.get(7));
            if(deletedDocs1 != null){
                v.set(3, docIdList1.size() - deletedDocs1.length);
            }
            if(deletedDocs2 != null){
                v.set(7, docIdList2.size() - deletedDocs2.length);
            }
        } else {
            // exist in either  1st/2nd segment
            if (v.get(0) == 0) {
                docIdList1 = segMgr1.readDocIdList(v.get(1), v.get(2), v.get(3));
                if(deletedDocs1 != null){
                    v.set(3, docIdList1.size() - deletedDocs1.length);
                }
            }
            else {
                docIdList2 = segMgr2.readDocIdList(v.get(1), v.get(2), v.get(3));
                if(deletedDocs2 != null){
                    v.set(3, docIdList2.size() - deletedDocs2.length);
                }
            }
        }

        //compare with deleted list if deleted then don't insert it and change the metadata of the slot
        if(deletedDocs1 != null)
        {
            docIdList1.removeAll(Arrays.asList(deletedDocs1));
        }

        if(deletedDocs2 != null)
        {
            docIdList2.removeAll(Arrays.asList(deletedDocs2));
        }
        // convert docId in segment 2
        for (int i = 0; i < docIdList2.size(); ++i) {
            int id_v = docIdList2.get(i);
            id_v += sz1;
            docIdList2.set(i, id_v);
        }

        // concat docId2 to docId1
        docIdList1.addAll(docIdList2);

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

        // rename segment
        f1 = new File(idxFolder + "segment_mergedSegment");
        f2 = new File(idxFolder + "segment_" + id1 / 2);
        boolean success = f1.renameTo(f2);

        if (!success) throw new UnsupportedOperationException("rename segment fail");

        f1 = new File(idxFolder + "posting_mergedSegment");
        f2 = new File(idxFolder + "posting_" + id1 / 2);
        success = f1.renameTo(f2);

        if (!success) throw new UnsupportedOperationException("rename segment fail");


        // delete 2nd document store
        f2 = new File(idxFolder + "DocStore_" + id2);
        f2.delete();

        //delete 1st document store
        f1 = new File(idxFolder + "DocStore_" + id1);
        f1.delete();

        f1 = getDeletedFile(id1);
        // delete 1st deleted
        if(f1 != null) {
            f1.delete();
        }

        f2 = getDeletedFile(id2);
        // delete 2nd deleted
        if(f2 != null) {
            f2.delete();
        }

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
                List<Integer> postingList = searchSegment(files[i].getName().substring(8), keywords.get(j));
                if (searchOperation == SearchOperation.AND_SEARCH && j > 0) {
                    postingListset.retainAll(postingList);
                } else {
                    postingListset.addAll(postingList);
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

    private List<Integer> searchSegment(String segment, String keyword) {
        List<Integer> postingList = new ArrayList<>();
        SegmentInDiskManager segMgr = new SegmentInDiskManager(idxFolder, segment);
        segMgr.readInitiate();
        Map<String, List<Integer>> dictMap = new TreeMap<>();
        while (segMgr.hasKeyWord()) {
            List<Integer> l1 = new ArrayList<>();
            String k1 = segMgr.readKeywordAndDict(l1);
            dictMap.put(k1, l1);
        }

        segMgr.readPostingInitiate();
        if (dictMap.containsKey(keyword)) {
            postingList = segMgr.readDocIdList(dictMap.get(keyword).get(0),
                    dictMap.get(keyword).get(1), dictMap.get(keyword).get(2));
        }
        return postingList;
    }

    private boolean containsFile(final List<File> list, final String fileName) {
        return list.stream().filter(o -> o.getName().substring(0, o.getName().indexOf("-")).equals(fileName)).findFirst().isPresent();
    }

    private int[] getDeletedDocsList(int segmentNumber) {
        int[] deletedDocs = null;
        File file = getDeletedFile(segmentNumber);
        PageFileChannel pfc = PageFileChannel.createOrOpen(file.toPath());
        ByteBuffer bb = pfc.readAllPages();
        int listLength = Integer.parseInt(file.getName().substring(file.getName().indexOf("-")+1));
        deletedDocs = new int[listLength];
        bb.position(0);
        for (int j = 0; j < deletedDocs.length; j++) {
            deletedDocs[j] = bb.getInt();
        }
        return deletedDocs;
    }

    private File getDeletedFile(int segmentNumber){
        File file = null;
        File dir = new File(idxFolder);
        File[] files = dir.listFiles((d, name) -> name.startsWith("deleted_" + segmentNumber + "-"));
        if(files.length >= 1){
            file = files[0];
        }
        return file;
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

    private int getNumOfDocs(int segmentNum) {
        int num = 0;
        DocumentStore mapDBIt = MapdbDocStore.createOrOpenReadOnly(idxFolder + "DocStore_" + segmentNum);
        num = (int) mapDBIt.size();
        mapDBIt.close();
        return num;
    }

    private boolean contains(final int[] arr, final int key) {
        return Arrays.stream(arr).anyMatch(i -> i == key);
    }
}
