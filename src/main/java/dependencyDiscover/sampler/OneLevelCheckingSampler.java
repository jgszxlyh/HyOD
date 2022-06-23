package dependencyDiscover.sampler;


import dependencyDiscover.Data.DataFrame;
import dependencyDiscover.Data.PartialDataFrame;
import dependencyDiscover.Predicate.Operator;
import dependencyDiscover.Predicate.SingleAttributePredicate;
import fastod.AttributeSet;
import fastod.StrippedPartition;

import java.util.HashSet;
import java.util.Set;

public class OneLevelCheckingSampler extends Sampler {
    private static final int LOW_BOUND = 10;
    private static final int UPPER_BOUND = 100;
    long seed = -1;

    @Override
    protected Set<Integer> chooseLines(DataFrame data, int adviceSampleSize) {
        Set<Integer> result = new HashSet<>();
        int rowCount = data.getRowCount();
        int columnCount = data.getColumnCount();
        if (rowCount <= LOW_BOUND) {
            for (int i = 0; i < rowCount; i++) {
                result.add(i);
            }
            return result;
        }

        int candidateSize = Math.min(rowCount, UPPER_BOUND);
        PartialDataFrame candidateData;
        RandomSampler sampler = new RandomSampler();
        if (seed != -1) {
            sampler.setRandomSeed(seed);
        }
        candidateData = sampler.sample(data, new SampleConfig(candidateSize));
        System.out.println("随机抽样抽样数量： " + candidateSize);

        StrippedPartition sp =new StrippedPartition(candidateData);
        //对fd进行第一层检查并把viorows加入样本集
        for (int c1 = 0; c1 < columnCount; c1++) {
            for (int c2 = c1 + 1; c2 < columnCount; c2++) {
                StrippedPartition res = sp.product(c2);
                result.addAll(res.splitForViorows(c1));
            }
        }
        //对OCD做上述操作
        for (int c1 = 0; c1 < columnCount; c1++) {
            for (int c2 = c1 + 1; c2 < columnCount; c2++) {
                //od的lhs为[c1]，rhs为[c2] 验证该od是否成立，并且记录违反的元组
                //这些violationRows的索引是对于随机抽样得到的数据集的也就是candidateData
                //不是对于原数据集r上的
                SingleAttributePredicate left1  = new SingleAttributePredicate(c1, Operator.lessEqual);
                SingleAttributePredicate left2  = new SingleAttributePredicate(c1, Operator.greaterEqual);
                result.addAll(sp.swapForViorows(left1,c2));
                result.addAll(sp.swapForViorows(left2,c2));

            }
        }
        Set<Integer> realResult = new HashSet<>();
        //result是在randomSample candidateData上的索引，而不是对于原数据集r上的索引
        System.out.println(result);
        //根据随机抽样得到的candidateData的索引得到原数据集r上的索引
//        realResult.addAll(result);
        return result;
    }

    @Override
    public void setRandomSeed(long randomSeed) {
        seed = randomSeed;
    }
}
