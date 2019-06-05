package edu.uci.ics.cs221.search;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.Node;
import edu.uci.ics.cs221.index.inverted.Pair;
import edu.uci.ics.cs221.storage.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static java.util.stream.Collectors.toList;

public class IcsSearchEngine {

    private static Path docDir;
    private static InvertedIndexManager ii;
    private HashMap<Integer, Node> nodes;
    /**
     * Initializes an IcsSearchEngine from the directory containing the documents and the
     *
     */
    public static IcsSearchEngine createSearchEngine(Path documentDirectory, InvertedIndexManager indexManager) {

        return new IcsSearchEngine(documentDirectory, indexManager);
    }

    private IcsSearchEngine(Path documentDirectory, InvertedIndexManager indexManager) {
        docDir = documentDirectory;
        ii = indexManager;
        nodes = new HashMap<>();
        writeIndex();
    }

    /**
     * Writes all ICS web page documents in the document directory to the inverted index.
     */
    public void writeIndex() {
        File dir = new File(docDir.toString()+"/cleaned");
        File[] files = dir.listFiles();
        sort(files);
        for(int i = 0; i < files.length; i++){
            try {
                String text = new String(Files.readAllBytes(files[i].toPath()), StandardCharsets.UTF_8);
                ii.addDocument(new Document(text));
            }
            catch (IOException e) {
                e.printStackTrace();
            }

        }
        ii.flush();
        computePageRank();
    }

    /**
     * Computes the page rank score from the "id-graph.tsv" file in the document directory.
     * The results of the computation can be saved in a class variable and will be later retrieved by `getPageRankScores`.
     */
    public void computePageRank() {
        int numOfIterations = 5;
        double dampingFactor = 0.85;
        File file = new File(docDir.toString() + "/id-graph.tsv");

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String st;
            List<Integer> edge;
            Integer source, target;
            while ((st = br.readLine()) != null){
                edge = Arrays.stream(st.split("\\s+")).map(Integer::parseInt).collect(toList());
                source = edge.get(0);
                target = edge.get(1);
                Node sourceNode = new Node();
                Node stargetNode = new Node();
                if(nodes.containsKey(source)) {
                    sourceNode = nodes.get(source);
                }
                sourceNode.setOutgoingSize(sourceNode.getOutgoingSize() + 1);
                    nodes.put(source, sourceNode);
                if(nodes.containsKey(target)) {
                    stargetNode = nodes.get(target);
                }
                stargetNode.addIncomingNode(source);
                nodes.put(target, stargetNode);
            }
            br.close();
            //loop through number of iterations
            for(int i = 0; i < numOfIterations; i++){
                double currentScore, incomingScores = 0;
                //loop through all pages
                for (Map.Entry<Integer, Node> entry : nodes.entrySet()) {
                    Node page = entry.getValue();
                    //to calculate current score, PR(P) = (1-d) + d ∑ (PR(Pi)/C(Pi))
                    currentScore = 1 - dampingFactor;
                    //for every page loop through all its incoming edges
                    List<Integer> incoming = page.getIncoming();
                    for(int j = 0; j < incoming.size(); j++){
                        incomingScores+= calculateIncomingScore(incoming.get(j));
                    }
                    currentScore += dampingFactor * incomingScores;
                    page.setCurrentScore(currentScore);
                    nodes.put(entry.getKey(), page);
                }
                setPreviousScore();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets the page rank score of all documents previously computed. Must be called after `cmoputePageRank`.
     * Returns an list of <DocumentID - Score> Pairs that is sorted by score in descending order (high scores first).
     */
    public List<Pair<Integer, Double>> getPageRankScores() {
        //make sure data structure is not null
        Preconditions.checkArgument(nodes.size() == 0);
        List<Pair<Integer, Double>> docs = new ArrayList<Pair<Integer, Double>>();
        for (Map.Entry<Integer, Node> entry : nodes.entrySet()) {
            docs.add(new Pair(new Document(getDocumentText(entry.getKey())), entry.getValue().getCurrentScore()));
        }
        Collections.sort(docs, Comparator.comparing(p -> p.getRight()));
        return docs;
    }

    /**
     * Searches the ICS document corpus and returns the top K documents ranked by combining TF-IDF and PageRank.
     *
     * The search process should first retrieve ALL the top documents from the InvertedIndex by TF-IDF rank,
     * by calling `searchTfIdf(query, null)`.
     *
     * Then the corresponding PageRank score of each document should be retrieved. (`computePageRank` will be called beforehand)
     * For each document, the combined score is  tfIdfScore + pageRankWeight * pageRankScore.
     *
     * Finally, the top K documents of the combined score are returned. Each element is a pair of <Document, combinedScore>
     *
     *
     * Note: We could get the Document ID by reading the first line of the document.
     * This is a workaround because our project doesn't support multiple fields. We cannot keep the documentID in a separate column.
     */
    public Iterator<Pair<Document, Double>> searchQuery(List<String> query, int topK, double pageRankWeight) {
        Iterator<Pair<Document, Double>> iterator = ii.searchTfIdf(query, null);
        //List<Pair<Integer, Double>> pageRankScores = getPageRankScores();
        PriorityQueue<Pair<Document, Double>> documents = new PriorityQueue<>(topK);
        double tfidfScore, pageRankScore;
        while(iterator.hasNext()){
            Pair<Document, Double> doc = iterator.next();
            String docText = doc.getLeft().getText();
            int docId = Integer.parseInt(docText.substring(0, docText.indexOf("\n") + 1));
            tfidfScore = doc.getRight();
            pageRankScore = nodes.get(docId).getCurrentScore();//getPageRankScore(docId, pageRankScores);//get from the list pageRankScores
            //add the score and add to priority queue
            documents.add(new Pair<>(doc.getLeft(), tfidfScore + pageRankWeight * pageRankScore));
        }
        return documents.iterator();
    }
    /**===========helper functions================*/

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

    private String getDocumentText(Integer docId){
        String str = "";
        File file = new File(docDir.toString()+"/cleaned/" + docId);
        try {
            str = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
            str = str.substring(str.indexOf('\n')+1);//remove first line for docId
            str = str.substring(str.indexOf('\n')+1);//remove second line for url
        } catch (IOException e) {
            e.printStackTrace();
        }
        return str;
    }

    private double calculateIncomingScore(Integer docId){
        Node node = nodes.get(docId);
        return node.getPrevScore() / node.getOutgoingSize();
    }

    private void setPreviousScore(){
        for (Map.Entry<Integer, Node> entry : nodes.entrySet()) {
            Node page = entry.getValue();
            page.setPrevScore(page.getCurrentScore());
            nodes.put(entry.getKey(), page);
        }
    }

   /* private double getPageRankScore(int docId, List<Pair<Integer, Double>> pageRankScores){
        double score = 0;

        return score;
    }*/
}
