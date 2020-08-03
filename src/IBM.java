import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

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

        HashMap<String, Integer> srcWordCount = new HashMap<>();
        HashMap<String, Integer> dstWordCount = new HashMap<>();


        while ((srcLine = srcReader.readLine()) != null && (dstLine = dstReader.readLine()) != null) {
            String[] srcWords = srcLine.trim().toLowerCase().split(" ");
            String[] dstWords = dstLine.trim().toLowerCase().split(" ");

            for (int i = 0; i < srcWords.length; i++) {
                String word = srcWords[i];
                if (!srcWordCount.containsKey(word)) {
                    srcWordCount.put(word, 1);
                } else {
                    srcWordCount.put(word, srcWordCount.get(word) + 1);
                }
            }

            for (int i = 0; i < dstWords.length; i++) {
                String word = dstWords[i];
                if (!dstWordCount.containsKey(word)) {
                    dstWordCount.put(word, 1);
                } else {
                    dstWordCount.put(word, dstWordCount.get(word) + 1);
                }
            }
            lineNum++;
            if (lineNum % 10000 == 0)
                System.out.print(lineNum + "\r");
        }
        srcReader.close();
        dstReader.close();


        for (String word : srcWordCount.keySet()) {
            if (srcWordCount.get(word) >= 10) {
                srcWord2ID.put(word, srcWord2ID.size());
                srcWordsList.add(word);
            }
        }

        for (String word : dstWordCount.keySet()) {
            if (dstWordCount.get(word) >= 50) {
                dstWord2ID.put(word, dstWord2ID.size());
                dstWordsList.add(word);
            }
        }
        srcWordCount = null;
        dstWordCount = null;
        System.gc();

        System.out.println("Constructed lexicons " + srcWordsList.size() + " " + dstWordsList.size());

        srcReader = new BufferedReader(new FileReader(args[0]));
        dstReader = new BufferedReader(new FileReader(args[1]));
        Pair<HashMap<Integer, Integer>, HashMap<Integer, Integer>>[] parallelData = new Pair[lineNum];
        int pNum = 0;
        while ((srcLine = srcReader.readLine()) != null && (dstLine = dstReader.readLine()) != null) {
            String[] srcWords = ("_null_ " + srcLine.trim().toLowerCase()).split(" ");
            String[] dstWords = dstLine.trim().toLowerCase().split(" ");

            HashMap<Integer, Integer> srcIds = new HashMap<>();
            for (String srcWord : srcWords) {
                if (srcWord2ID.containsKey(srcWord)) {
                    int srcID = srcWord2ID.get(srcWord);
                    if (!srcIds.containsKey(srcID))
                        srcIds.put(srcID, 1);
                    else
                        srcIds.put(srcID, srcIds.get(srcID) + 1);
                }
            }

            HashMap<Integer, Integer> dstIds = new HashMap<>();
            for (String dstWord : dstWords) {
                if (dstWord2ID.containsKey(dstWord)) {
                    int dstID = dstWord2ID.get(dstWord);
                    if (!dstIds.containsKey(dstID))
                        dstIds.put(dstID, 1);
                    else
                        dstIds.put(dstID, dstIds.get(dstID) + 1);
                }
            }
            parallelData[pNum] = new Pair<>(srcIds, dstIds);
            pNum++;
            if (pNum % 10000 == 0)
                System.out.print(pNum + "\r");
        }
        System.out.println("\nConstructed parallel data of size " + lineNum);

        HashMap<Integer, Float>[] translationProb = new HashMap[srcWordsList.size()];
        float initVal = 1.0f / srcWordsList.size();

        HashMap<Integer, Float>[] Q = new HashMap[srcWordsList.size()];
        float[] C = new float[srcWordsList.size()];
        Arrays.fill(C, 0.0f);

        for (int s = 0; s < srcWordsList.size(); s++) {
            translationProb[s] = new HashMap<>();
            Q[s] = new HashMap<>();
        }

        for (int iter = 0; iter < ibmIterations; iter++) {
            System.out.println("IBM Iter: " + (iter + 1));
            srcReader = new BufferedReader(new FileReader(args[0]));
            dstReader = new BufferedReader(new FileReader(args[1]));
            for (pNum = 0; pNum < parallelData.length; pNum++) {
                HashMap<Integer, Integer> srcIds = parallelData[pNum].first;
                HashMap<Integer, Integer> dstIds = parallelData[pNum].second;

                for (int srcID : srcIds.keySet()) {
                    HashMap<Integer, Float> tProb = translationProb[srcID];
                    HashMap<Integer, Float> qProb = Q[srcID];

                    int srcCount = srcIds.get(srcID);
                    float denom = 0.0f;
                    for (int dstId : dstIds.keySet()) {
                        if (!tProb.containsKey(dstId)) {
                            tProb.put(dstId, initVal);
                            qProb.put(dstId, 0.0f);
                        }

                        int dstCount = dstIds.get(dstId);
                        float prob = srcCount * dstCount * tProb.get(dstId);
                        denom += prob;
                    }
                    for (int dstId : dstIds.keySet()) {
                        int dstCount = dstIds.get(dstId);
                        float prob = (srcCount * dstCount * tProb.get(dstId)) / denom;
                        qProb.put(dstId, qProb.get(dstId) + prob);
                        C[srcID] += prob;
                    }
                }
                if (pNum % 1000 == 0)
                    System.out.print(pNum + "/" + lineNum + "\r");
            }
            srcReader.close();
            dstReader.close();

            System.out.println("\nRenewing probabilities");

            // Renew translation probabilities.
            for (int s = 0; s < srcWordsList.size(); s++) {
                HashMap<Integer, Float> qProb = Q[s];
                HashMap<Integer, Float> tProb = translationProb[s];
                float denom = C[s];
                for (int t : qProb.keySet()) {
                    float prob = qProb.get(t) / denom;
                    tProb.put(t, prob);
                }
            }

            // Reset Q and C
            Arrays.fill(C, 0.0f);
            for (int s = 0; s < srcWordsList.size(); s++) {
                HashMap<Integer, Float> qProb = Q[s];
                for (int t : qProb.keySet()) {
                    qProb.put(t, 0.0f);
                }
            }

            System.out.println("Writing non-zero probabilities");
            BufferedWriter probWriter = new BufferedWriter(new FileWriter(outputPath));
            int numWritten = 0;
            for (int s = 0; s < srcWordsList.size(); s++) {
                String srcWord = srcWordsList.get(s);
                HashMap<Integer, Float> tProb = translationProb[s];
                for (int t : tProb.keySet()) {
                    float prob = tProb.get(t);
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

        System.out.println("Writing top score elements");
        BufferedWriter probWriter = new BufferedWriter(new FileWriter(outputPath+".best"));

        for (int s = 0; s < srcWordsList.size(); s++) {
            String srcWord = srcWordsList.get(s);
            HashMap<Integer, Float> tProb = translationProb[s];

            TreeMap<Float, HashSet<Integer>> sortedMap = new TreeMap<>();

            for (int t : tProb.keySet()) {
                float prob = -tProb.get(t);
                if(!sortedMap.containsKey(prob)){
                    sortedMap.put(prob, new HashSet<>());
                }
                sortedMap.get(prob).add(t);
            }
            int used = 0;
            StringBuilder output = new StringBuilder();
            output.append(srcWord);

            for(float prob: sortedMap.keySet()) {
                for(int t:sortedMap.get(prob)){
                    String dstWord = dstWordsList.get(t);
                    output.append("\t");
                    output.append(dstWord);
                    output.append("\t");
                    output.append((-prob));
                }
                used ++;
                if(used>=5)
                    break;
            }
            output.append("\n");
            probWriter.write(output.toString());
        }
        probWriter.close();

    }
}
