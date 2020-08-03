import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

class Pair<T1, T2> {
    public T1 first;
    public T2 second;

    public Pair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }
}

public class IBM {
    public static void main(String[] args) throws Exception {
        BufferedReader srcReader = new BufferedReader(new FileReader(args[0]));
        BufferedReader dstReader = new BufferedReader(new FileReader(args[1]));
        int ibmIterations = Integer.parseInt(args[2]);
        String outputPath = args[3];

        HashMap<String, Integer> srcWord2ID = new HashMap<>();
        ArrayList<String> srcWordsList = new ArrayList<>();
        srcWordsList.add("_null_");
        int _null_ = 0;
        srcWord2ID.put("_null_", _null_);

        HashMap<String, Integer> dstWord2ID = new HashMap<>();
        ArrayList<String> dstWordsList = new ArrayList<>();

        String srcLine = null;
        String dstLine = null;

        System.out.println("Constructing parallel data...");
        int lineNum = 0;
        while ((srcLine = srcReader.readLine()) != null && (dstLine = dstReader.readLine()) != null) {
            String[] srcWords = srcLine.trim().toLowerCase().split(" ");
            String[] dstWords = dstLine.trim().toLowerCase().split(" ");

            for (int i = 0; i < srcWords.length; i++) {
                String word = srcWords[i];
                if (!srcWord2ID.containsKey(word)) {
                    srcWordsList.add(word);
                    srcWord2ID.put(word, srcWord2ID.size());
                }
            }

            for (int i = 0; i < dstWords.length; i++) {
                String word = dstWords[i];
                if (!dstWord2ID.containsKey(word)) {
                    dstWordsList.add(word);
                    dstWord2ID.put(word, dstWord2ID.size());
                }
            }
            lineNum++;
            if (lineNum % 10000 == 0)
                System.out.print(lineNum + "\r");
        }
        srcReader.close();
        dstReader.close();
        System.out.println("\nConstructed parallel data of size " + lineNum);

        HashMap<Integer, HashMap<Integer, Double>> translationProb = new HashMap<>();
        double initVal = 1.0 / srcWordsList.size();

        HashMap<Integer, HashMap<Integer, Double>> Q = new HashMap<>();
        double[] C = new double[srcWordsList.size()];
        Arrays.fill(C, 0.0);


        for (int iter = 0; iter < ibmIterations; iter++) {
            System.out.println("IBM Iter: " + (iter + 1));
            srcReader = new BufferedReader(new FileReader(args[0]));
            dstReader = new BufferedReader(new FileReader(args[1]));
            int pNum = 0;
            while ((srcLine = srcReader.readLine()) != null && (dstLine = dstReader.readLine()) != null) {
                String[] srcWords = ("_null_ " + srcLine.trim().toLowerCase()).split(" ");
                String[] dstWords = dstLine.trim().toLowerCase().split(" ");

                HashMap<Integer, Integer> srcIds = new HashMap<>();
                for (String srcWord : srcWords) {
                    int srcID = srcWord2ID.get(srcWord);
                    if (!srcIds.containsKey(srcID))
                        srcIds.put(srcID, 1);
                    else
                        srcIds.put(srcID, srcIds.get(srcID) + 1);
                }

                HashMap<Integer, Integer> dstIds = new HashMap<>();
                for (String dstWord : dstWords) {
                    int dstID = dstWord2ID.get(dstWord);
                    if (!dstIds.containsKey(dstID))
                        dstIds.put(dstID, 1);
                    else
                        dstIds.put(dstID, dstIds.get(dstID) + 1);
                }

                for (int srcID : srcIds.keySet()) {
                    if (!translationProb.containsKey(srcID)) {
                        translationProb.put(srcID, new HashMap<>());
                        Q.put(srcID, new HashMap<>());
                    }

                    HashMap<Integer, Double> tProb = translationProb.get(srcID);
                    HashMap<Integer, Double> qProb = Q.get(srcID);

                    int srcCount = srcIds.get(srcID);
                    double denom = 0.0;
                    for (int dstId : dstIds.keySet()) {
                        if (!tProb.containsKey(dstId)) {
                            tProb.put(dstId, initVal);
                            qProb.put(dstId, 0.0);
                        }

                        int dstCount = dstIds.get(dstId);
                        double prob = srcCount * dstCount * tProb.get(dstId);
                        denom += prob;
                    }
                    for (int dstId : dstIds.keySet()) {
                        int dstCount = dstIds.get(dstId);
                        double prob = (srcCount * dstCount * tProb.get(dstId)) / denom;
                        qProb.put(dstId, qProb.get(dstId) + prob);
                        C[srcID] += prob;
                    }
                }
                pNum++;
                if (pNum % 1000 == 0)
                    System.out.print(pNum + "/" + lineNum + "\r");
            }
            srcReader.close();
            dstReader.close();

            System.out.println("\nRenewing probabilities");

            // Renew translation probabilities.
            for (int s : Q.keySet()) {
                HashMap<Integer, Double> qProb = Q.get(s);
                HashMap<Integer, Double> tProb = translationProb.get(s);
                double denom = C[s];
                for (int t : qProb.keySet()) {
                    double prob = qProb.get(t) / denom;
                    tProb.put(t, prob);

                }
            }


            // Reset Q and C
            Arrays.fill(C, 0.0);
            for (int s : Q.keySet()) {
                for (int t : Q.get(s).keySet()) {
                    Q.get(s).put(t, 0.0);
                }
            }

            System.out.println("Writing non-zero probabilities");
            BufferedWriter probWriter = new BufferedWriter(new FileWriter(outputPath));
            int numWritten = 0;
            for (int s : translationProb.keySet()) {
                String srcWord = srcWordsList.get(s);
                HashMap<Integer, Double> tProb = translationProb.get(s);
                for (int t : tProb.keySet()) {
                    double prob = tProb.get(t);
                    if (prob > 0) {
                        String dstWord = dstWordsList.get(t);
                        probWriter.write(srcWord + "\t" + dstWord + "\t" + prob + "\n");
                        numWritten++;
                    }
                }
            }
            System.out.println("Wrote " + numWritten + " non-zero probabilities");

            probWriter.close();
        }

    }
}
