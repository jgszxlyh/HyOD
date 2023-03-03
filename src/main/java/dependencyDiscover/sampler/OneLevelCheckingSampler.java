package dependencydiscover.sampler;

import hyod.StrippedPartition;
import dependencydiscover.dataframe.DataFrame;
import dependencydiscover.dataframe.PartialDataFrame;
import dependencydiscover.predicate.Operator;
import dependencydiscover.predicate.SingleAttributePredicate;

import java.util.*;

public class OneLevelCheckingSampler extends Sampler {
    private static final int LOW_BOUND = 10;
    private static final int UPPER_BOUND = 100;
    long seed = -1;

    @Override
    protected Set<Integer> chooseLines(DataFrame data, int adviceSampleSize) {
        Set<Integer> result = new HashSet<>();
        HashMap<Integer, Integer> index2count = new HashMap<>();
        int rowCount = data.getRowCount();
        int columnCount = data.getColumnCount();
        if (rowCount <= LOW_BOUND) {
            for (int i = 0; i < rowCount; i++) {
                result.add(i);
            }
            return result;
        }

        int candidateSize = Math.min(rowCount, adviceSampleSize);
        PartialDataFrame candidateData;
        RandomSampler sampler = new RandomSampler();
        if (seed != -1) {
            sampler.setRandomSeed(seed);
        }
        candidateData = sampler.sample(data, new SampleConfig(candidateSize));
        System.out.println("random sample size: " + candidateSize);
        StrippedPartition sp = new StrippedPartition(candidateData);

        for (int c1 = 0; c1 < columnCount; c1++) {
            HashSet<Integer> temp = new HashSet<>(sp.splitForSample(c1, 2));
            for (int i : temp) {
                if (index2count.containsKey(i))
                    index2count.replace(i, index2count.get(i) + 1);
                else
                    index2count.put(i, 1);
            }
        }

        for (int c1 = 0; c1 < columnCount; c1++) {
            for (int c2 = c1 + 1; c2 < columnCount; c2++) {

                SingleAttributePredicate left1 = new SingleAttributePredicate(c1, Operator.LESSEQUAL);
                SingleAttributePredicate left2 = new SingleAttributePredicate(c1, Operator.GREATEREQUAL);
                HashSet<Integer> temp = new HashSet<>(sp.swapForSample(left1, c2, 2));
                temp.addAll(sp.swapForSample(left2, c2, 10));
                for (int i : temp) {
                    if (index2count.containsKey(i))
                        index2count.replace(i, index2count.get(i) + 1);
                    else
                        index2count.put(i, 1);
                }

            }
        }
        List<Map.Entry<Integer, Integer>> r = new ArrayList<Map.Entry<Integer, Integer>>(index2count.entrySet());
        r.sort(new Comparator<Map.Entry<Integer, Integer>>() {
            @Override
            public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                return o2.getValue() - o1.getValue();
            }
        });
        for (Map.Entry<Integer, Integer> integerIntegerEntry : r) {
            result.add(integerIntegerEntry.getKey());
        }

        Set<Integer> realResult = new HashSet<>();

        for (Integer i : result) {

            realResult.add(candidateData.getIndexInOriginalDataFrame(i));
        }

        return realResult;
    }

    @Override
    public void setRandomSeed(long randomSeed) {
        seed = randomSeed;
    }

    public static void main(String[] args) {
        DataFrame data = DataFrame.fromCsv("Data/Feb_2020_ontime-int 14.csv");

        OneLevelCheckingSampler sampler = new OneLevelCheckingSampler();
        PartialDataFrame data1 = sampler.sample(data);
        PartialDataFrame data2 = sampler.sample(data);
        PartialDataFrame data3 = sampler.sample(data);

        System.out.println("data1:" + data1.getData());
        System.out.println("data2:" + data2.getData());
        System.out.println("data3:" + data3.getData());

        System.exit(0);
    }
}
