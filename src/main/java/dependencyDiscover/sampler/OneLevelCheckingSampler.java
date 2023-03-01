package dependencyDiscover.sampler;


import dependencyDiscover.Data.DataFrame;
import dependencyDiscover.Data.PartialDataFrame;
import dependencyDiscover.Predicate.Operator;
import dependencyDiscover.Predicate.SingleAttributePredicate;
import HyOD.StrippedPartition;

import java.util.*;

public class OneLevelCheckingSampler extends Sampler {
    private static final int LOW_BOUND = 10;
    private static final int UPPER_BOUND = 100;
    long seed = -1;

    @Override
    protected Set<Integer> chooseLines(DataFrame data, int adviceSampleSize) {
        Set<Integer> result = new HashSet<>();
        HashMap<Integer,Integer> index2count = new HashMap<>();
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
        System.out.println("随机抽样抽样数量： " + candidateSize);
        StrippedPartition sp =new StrippedPartition(candidateData);
        //对fd进行第一层检查并把viorows加入样本集
        for (int c1 = 0; c1 < columnCount; c1++) {
            HashSet<Integer> temp = new HashSet<>(sp.splitForSample(c1, 2));
            for(int i:temp) {
                if (index2count.containsKey(i))
                    index2count.replace(i, index2count.get(i) + 1);
                else
                    index2count.put(i, 1);
            }
        }
        //对OCD做上述操作

        for (int c1 = 0; c1 < columnCount; c1++) {
            for (int c2 = c1 + 1; c2  < columnCount; c2++) {
                //od的lhs为[c1]，rhs为[c2] 验证该od是否成立，并且记录违反的元组
                //这些violationRows的索引是对于随机抽样得到的数据集的也就是candidateData
                //不是对于原数据集r上的
                SingleAttributePredicate left1  = new SingleAttributePredicate(c1, Operator.lessEqual);
                SingleAttributePredicate left2  = new SingleAttributePredicate(c1, Operator.greaterEqual);
                HashSet<Integer> temp = new HashSet<>(sp.swapForSample(left1,c2,2));
                temp.addAll(sp.swapForSample(left2,c2,10));
                for(int i:temp){
                    if(index2count.containsKey(i))
                        index2count.replace(i,index2count.get(i)+1);
                    else
                        index2count.put(i,1);
                }

            }
        }
        List<Map.Entry<Integer,Integer>> r = new ArrayList<Map.Entry<Integer,Integer>>(index2count.entrySet());
        r.sort(new Comparator<Map.Entry<Integer, Integer>>() {
            @Override
            public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                return o2.getValue() - o1.getValue();
            }
        });
        for (Map.Entry<Integer, Integer> integerIntegerEntry : r) {
            result.add(integerIntegerEntry.getKey());
        }
        //对od进行最后一层检查，得到最有效的non—od并把viorows加入样本集
//        for (int c1 = 0; c1 < columnCount; c1++) {
//            for(int i=0;i<columnCount&&i!=c1;i++){
//                sp.product(i);
//            }
//            result.addAll(sp.splitForViorows(c1));
//        }
//        for (int c1 = 0; c1 < columnCount; c1++) {
//            for (int c2 = c1 + 1; c2 < columnCount; c2++) {
//                //od的lhs为[c1]，rhs为[c2] 验证该od是否成立，并且记录违反的元组
//                //这些violationRows的索引是对于随机抽样得到的数据集的也就是candidateData
//                //不是对于原数据集r上的
//                for(int i=0;i<columnCount&&i!=c1&&i!=c2;i++){
//                    sp.product(i);
//                }
//                SingleAttributePredicate left1  = new SingleAttributePredicate(c1, Operator.lessEqual);
//                SingleAttributePredicate left2  = new SingleAttributePredicate(c1, Operator.greaterEqual);
//                result.addAll(sp.swapForViorows(left1,c2));
//                result.addAll(sp.swapForViorows(left2,c2));
//
//            }
//        }
//        //倒数第二层
//        for (int c1 = 0; c1 < columnCount; c1++) {
//            for (int c2 = c1 + 1; c2 < columnCount; c2++) {
//                for (int i = 0; i < columnCount ; i++) {
//                    if(i != c1&&i!=c2 )
//                        sp.product(i);
//                    }
//                    result.addAll(sp.splitForViorows(c1));
//                }
//        }
//        for (int c1 = 0; c1 < columnCount; c1++) {
//            for (int c2 = c1 + 1; c2 < columnCount; c2++) {
//                for (int c3 = c1 + 2; c3 < columnCount; c3++) {
//                    //od的lhs为[c1]，rhs为[c2] 验证该od是否成立，并且记录违反的元组
//                    //这些violationRows的索引是对于随机抽样得到的数据集的也就是candidateData
//                    //不是对于原数据集r上的
//                    for (int i = 0; (i < columnCount); i++) {
//                        if ((i != c1) && (i != c2) && (i != c3))
//                            sp.product(i);
//                    }
//                    SingleAttributePredicate left1 = new SingleAttributePredicate(c1, Operator.lessEqual);
//                    SingleAttributePredicate left2 = new SingleAttributePredicate(c1, Operator.greaterEqual);
//                    result.addAll(sp.swapForViorows(left1, c2));
//                    result.addAll(sp.swapForViorows(left2, c2));
//                }
//            }
//        }
        Set<Integer> realResult = new HashSet<>();
        //result是在randomSample candidateData上的索引，而不是对于原数据集r上的索引
        //根据随机抽样得到的candidateData的索引得到原数据集r上的索引
        for (Integer i : result) {
            //根据随机抽样得到的candidateData的索引得到原数据集r上的索引
            realResult.add(candidateData.getIndexInOriginalDataFrame(i));
        }
//        System.out.println(realResult);
//        for(int i:result)
//            System.out.println(data.getTuple(i));
//        System.out.println(result.size());
        return realResult;
    }

    @Override
    public void setRandomSeed(long randomSeed) {
        seed = randomSeed;
    }
    public static void main(String[] args) {
        DataFrame data = DataFrame.fromCsv("Data/Feb_2020_ontime-int 14.csv");
//        DataFrame data = DataFrame.fromCsv("./data/test2.csv");
        OneLevelCheckingSampler sampler = new OneLevelCheckingSampler();
        PartialDataFrame data1 = sampler.sample(data);
        PartialDataFrame data2 = sampler.sample(data);
        PartialDataFrame data3 = sampler.sample(data);

        System.out.println("data1:"+data1.getData());
        System.out.println("data2:"+data2.getData());
        System.out.println("data3:"+data3.getData());

//        System.out.println(f.setOD2ListOD(result));
        System.exit(0);
    }
}
