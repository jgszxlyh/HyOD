package hyod;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import dependencydiscover.dataframe.DataFrame;
import dependencydiscover.predicate.Operator;
import dependencydiscover.predicate.SingleAttributePredicate;
import util.Timer;

public class StrippedPartition {
    private List<Integer> indexes;

    private List<Integer> begins;
    private DataFrame data;
    public static long mergeTime = 0;
    public static long validateTime = 0;
    public static long cloneTime = 0;
    public static final int CACHE_SIZE = 10000;

    private static final ConcurrentHashMap<AttributeSet, StrippedPartition> sampleCache = new ConcurrentHashMap<AttributeSet, StrippedPartition>() {
    };

    private static final ConcurrentHashMap<AttributeSet, StrippedPartition> cache = new ConcurrentHashMap<AttributeSet, StrippedPartition>() {
    };

    public StrippedPartition() {
    }

    public StrippedPartition(DataFrame data) {
        this.data = data;

        indexes = new ArrayList<>();
        for (int i = 0; i < data.getTupleCount(); i++) {
            indexes.add(i);
        }
        begins = new ArrayList<>();
        if (data.getTupleCount() != 0) {
            begins.add(0);
        }
        begins.add(data.getTupleCount());
    }

    public StrippedPartition(StrippedPartition origin) {
        this.indexes = new ArrayList<>(origin.indexes);
        this.begins = new ArrayList<>(origin.begins);
        this.data = origin.data;
    }

    public StrippedPartition product(int attribute) {
        Timer timer = new Timer();
        List<Integer> newIndexes = new ArrayList<>();
        List<Integer> newBegins = new ArrayList<>();
        int fillPointer = 0;

        for (int beginPointer = 0; beginPointer < begins.size() - 1; beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);
            HashMap<Integer, List<Integer>> subGroups = new HashMap<>();

            for (int i = groupBegin; i < groupEnd; i++) {
                int index = indexes.get(i);
                int value = data.get(index, attribute);

                if (!subGroups.containsKey(value)) {
                    subGroups.put(value, new ArrayList<>());

                }
                subGroups.get(value).add(index);
            }

            for (List<Integer> newGroup : subGroups.values()) {
                if (newGroup.size() >= 1) {
                    newBegins.add(fillPointer);
                    for (int i : newGroup) {
                        newIndexes.add(i);
                        fillPointer++;
                    }
                }
            }
        }
        this.indexes = newIndexes;

        this.begins = newBegins;
        begins.add(indexes.size());

        mergeTime += timer.getTimeUsed();
        return this;
    }

    public boolean split(int right) {
        Timer timer = new Timer();
        for (int beginPointer = 0; beginPointer < begins.size() - 1; beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);

            int groupValue = data.get(indexes.get(groupBegin), right);
            for (int i = groupBegin + 1; i < groupEnd; i++) {
                int index = indexes.get(i);
                int value = data.get(index, right);
                if (value != groupValue) {
                    validateTime += timer.getTimeUsed();
                    return true;
                }
            }
        }
        validateTime += timer.getTimeUsed();
        return false;
    }

    public Set<Integer> splitForViorows(int right) {
        Timer timer = new Timer();
        Set<Integer> viorows = new HashSet<>();

        for (int beginPointer = 0; beginPointer < begins.size() - 1; beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);

            int groupValue = data.get(indexes.get(groupBegin), right);
            for (int i = groupBegin + 1; i < groupEnd; i++) {
                int index = indexes.get(i);
                int value = data.get(index, right);
                if (value != groupValue) {
                    validateTime += timer.getTimeUsed();
                    viorows.add(index);
                    viorows.add(indexes.get(groupBegin));
                    return viorows;
                }
            }
        }
        validateTime += timer.getTimeUsed();
        return viorows;
    }

    public Set<Integer> splitForSample(int right, int vioNum) {
        Timer timer = new Timer();
        HashMap<Integer, Integer> index2length = new HashMap<>();
        HashMap<Integer, Integer> index2vioindex = new HashMap<>();
        ArrayList<Integer> vioindex = new ArrayList<>();
        int minlength = 0;
        int minindex = 0;
        Set<Integer> viorows = new HashSet<>();
        for (int beginPointer = 0; beginPointer < begins.size() - 1; beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);

            int groupValue = data.get(indexes.get(groupBegin), right);
            for (int i = groupBegin + 1; i < groupEnd; i++) {
                int index = indexes.get(i);
                int value = data.get(index, right);
                if (value != groupValue) {
                    int temp = trueContextLength(index, indexes.get(groupBegin), right);
                    if (vioindex.size() < vioNum) {
                        vioindex.add(index);
                        index2vioindex.put(index, indexes.get(groupBegin));
                        index2length.put(index, temp);
                        if (temp < minlength) {
                            minindex = index;
                            minlength = temp;
                        }
                    } else if (temp > minlength) {
                        vioindex.remove(new Integer(minindex));
                        index2vioindex.remove(minindex);
                        index2length.remove(minindex);
                        vioindex.add(index);
                        index2vioindex.put(index, indexes.get(groupBegin));
                        index2length.put(index, temp);
                        minlength = temp;
                        minindex = index;
                        for (int k : vioindex) {
                            if (index2length.get(k) < minlength) {
                                minindex = k;
                                minlength = index2length.get(k);
                            }
                        }
                    }
                }
            }
        }
        for (int index : vioindex) {
            viorows.add(index);
            viorows.add(index2vioindex.get(index));
        }
        validateTime += timer.getTimeUsed();
        return viorows;
    }

    public Set<Integer> splitForRandom(int right) {
        Timer timer = new Timer();
        List<Integer> viorows = new ArrayList<>();

        for (int beginPointer = 0; beginPointer < begins.size() - 1; beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);

            int groupValue = data.get(indexes.get(groupBegin), right);
            for (int i = groupBegin + 1; i < groupEnd; i++) {
                int index = indexes.get(i);
                int value = data.get(index, right);
                if (value != groupValue) {
                    validateTime += timer.getTimeUsed();
                    viorows.add(index);
                    viorows.add(indexes.get(groupBegin));

                }
            }
        }
        validateTime += timer.getTimeUsed();
        Set<Integer> vio = new HashSet<>();
        Random random = new Random();
        if (!viorows.isEmpty()) {
            for (int i = 0; i < 10; i++) {
                vio.add(viorows.remove(random.nextInt(viorows.size())));
                if (viorows.isEmpty())
                    break;
            }
        }
        return vio;
    }

    public boolean swap(SingleAttributePredicate left, int right) {
        Timer timer = new Timer();
        for (int beginPointer = 0; beginPointer < begins.size() - 1; beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);

            List<DataAndDataAndIndex> values = new ArrayList<>();
            for (int i = groupBegin; i < groupEnd; i++) {
                int index = indexes.get(i);
                values.add(new DataAndDataAndIndex(filteredDataFrameGet(data, index, left), data.get(index, right),
                        index));
            }

            Collections.sort(values);
            int beforeMax = Integer.MIN_VALUE;
            int groupMax = Integer.MIN_VALUE;
            int beforeMaxIndex = -1;
            int groupMaxIndex = -1;
            for (int i = 0; i < values.size(); i++) {
                int r = values.get(i).dataRight;
                if (i == 0 || values.get(i - 1).dataLeft != values.get(i).dataLeft) {

                    if (beforeMax < groupMax) {
                        beforeMax = groupMax;
                        beforeMaxIndex = groupMaxIndex;
                    }
                    groupMax = r;
                    groupMaxIndex = values.get(i).index;
                } else {
                    groupMax = Math.max(groupMax, r);
                    if (groupMax == r)
                        groupMaxIndex = values.get(i).index;
                }
                if (r < beforeMax) {
                    validateTime += timer.getTimeUsed();
                    return true;
                }
            }
        }
        validateTime += timer.getTimeUsed();
        return false;
    }

    public Set<Integer> swapForViorows(SingleAttributePredicate left, int right) {
        Timer timer = new Timer();
        Set<Integer> viorows = new HashSet<>();
        for (int beginPointer = 0; beginPointer < begins.size() - 1; beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);

            List<DataAndDataAndIndex> values = new ArrayList<>();
            for (int i = groupBegin; i < groupEnd; i++) {
                int index = indexes.get(i);
                values.add(new DataAndDataAndIndex(filteredDataFrameGet(data, index, left), data.get(index, right),
                        index));
            }

            Collections.sort(values);
            int beforeMax = Integer.MIN_VALUE;
            int groupMax = Integer.MIN_VALUE;
            int beforeMaxIndex = -1;
            int groupMaxIndex = -1;
            for (int i = 0; i < values.size(); i++) {
                int r = values.get(i).dataRight;
                if (i == 0 || values.get(i - 1).dataLeft != values.get(i).dataLeft) {

                    if (beforeMax < groupMax) {
                        beforeMax = groupMax;
                        beforeMaxIndex = groupMaxIndex;
                    }
                    groupMax = r;
                    groupMaxIndex = values.get(i).index;
                } else {
                    groupMax = Math.max(groupMax, r);
                    if (groupMax == r)
                        groupMaxIndex = values.get(i).index;
                }
                if (r < beforeMax) {
                    validateTime += timer.getTimeUsed();
                    viorows.add(values.get(i).index);
                    viorows.add(beforeMaxIndex);
                    return viorows;
                }
            }
        }
        validateTime += timer.getTimeUsed();
        return viorows;
    }

    public Set<Integer> swapForSample(SingleAttributePredicate left, int right, int vioNum) {
        Timer timer = new Timer();
        Set<Integer> viorows = new HashSet<>();
        HashMap<Integer, Integer> index2length = new HashMap<>();
        HashMap<Integer, Integer> index2vioindex = new HashMap<>();
        ArrayList<Integer> vioindex = new ArrayList<>();
        int minlength = 0;
        int minindex = 0;
        for (int beginPointer = 0; beginPointer < begins.size() - 1; beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);

            List<DataAndDataAndIndex> values = new ArrayList<>();
            for (int i = groupBegin; i < groupEnd; i++) {
                int index = indexes.get(i);
                values.add(new DataAndDataAndIndex(filteredDataFrameGet(data, index, left), data.get(index, right),
                        index));
            }

            Collections.sort(values);
            int beforeMax = Integer.MIN_VALUE;
            int groupMax = Integer.MIN_VALUE;
            int beforeMaxIndex = -1;
            int groupMaxIndex = -1;
            for (int i = 0; i < values.size(); i++) {
                int r = values.get(i).dataRight;
                if (i == 0 || values.get(i - 1).dataLeft != values.get(i).dataLeft) {

                    if (beforeMax < groupMax) {
                        beforeMax = groupMax;
                        beforeMaxIndex = groupMaxIndex;
                    }
                    groupMax = r;
                    groupMaxIndex = values.get(i).index;
                } else {
                    groupMax = Math.max(groupMax, r);
                    if (groupMax == r)
                        groupMaxIndex = values.get(i).index;
                }
                if (r < beforeMax) {
                    int temp = trueContextLength(values.get(i).index, beforeMaxIndex, right, left.attribute);
                    if (vioindex.size() < vioNum) {
                        vioindex.add(values.get(i).index);
                        index2vioindex.put(values.get(i).index, beforeMaxIndex);
                        index2length.put(values.get(i).index, temp);
                        if (temp < minlength) {
                            minindex = values.get(i).index;
                            minlength = temp;
                        }
                    } else if (temp > minlength) {
                        vioindex.remove(Integer.valueOf(minindex));
                        index2vioindex.remove(minindex);
                        index2length.remove(minindex);
                        vioindex.add(values.get(i).index);
                        index2vioindex.put(values.get(i).index, beforeMaxIndex);
                        index2length.put(values.get(i).index, temp);
                        minlength = temp;
                        minindex = values.get(i).index;
                        for (int index : vioindex) {
                            if (index2length.get(index) < minlength) {
                                minindex = index;
                                minlength = index2length.get(index);
                            }
                        }
                    }
                }
            }
        }
        for (int index : vioindex) {
            viorows.add(index);
            viorows.add(index2vioindex.get(index));
        }
        validateTime += timer.getTimeUsed();
        return viorows;
    }

    public Set<Integer> swapForRandom(SingleAttributePredicate left, int right) {
        Timer timer = new Timer();
        List<Integer> viorows = new ArrayList<>();
        for (int beginPointer = 0; beginPointer < begins.size() - 1; beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);

            List<DataAndDataAndIndex> values = new ArrayList<>();
            for (int i = groupBegin; i < groupEnd; i++) {
                int index = indexes.get(i);
                values.add(new DataAndDataAndIndex(filteredDataFrameGet(data, index, left), data.get(index, right),
                        index));
            }

            Collections.sort(values);
            int beforeMax = Integer.MIN_VALUE;
            int groupMax = Integer.MIN_VALUE;
            int beforeMaxIndex = -1;
            int groupMaxIndex = -1;
            for (int i = 0; i < values.size(); i++) {
                int r = values.get(i).dataRight;
                if (i == 0 || values.get(i - 1).dataLeft != values.get(i).dataLeft) {

                    if (beforeMax < groupMax) {
                        beforeMax = groupMax;
                        beforeMaxIndex = groupMaxIndex;
                    }
                    groupMax = r;
                    groupMaxIndex = values.get(i).index;
                } else {
                    groupMax = Math.max(groupMax, r);
                    if (groupMax == r)
                        groupMaxIndex = values.get(i).index;
                }
                if (r < beforeMax) {
                    validateTime += timer.getTimeUsed();
                    viorows.add(values.get(i).index);
                    viorows.add(beforeMaxIndex);
                }
            }
        }
        validateTime += timer.getTimeUsed();
        Set<Integer> vio = new HashSet<>();
        Random random = new Random();
        if (!viorows.isEmpty()) {
            for (int i = 0; i < 10; i++) {
                vio.add(viorows.remove(random.nextInt(viorows.size())));
                if (viorows.isEmpty())
                    break;
            }
        }
        return vio;
    }

    @Override
    public String toString() {
        return "StrippedPartition{" +
                "indexes=" + indexes +
                ", begins=" + begins +
                '}';
    }

    public StrippedPartition deepClone() {
        Timer timer = new Timer();
        StrippedPartition result = new StrippedPartition(this.data);
        result.indexes = new ArrayList<>(indexes);
        result.begins = new ArrayList<>(begins);
        cloneTime += timer.getTimeUsed();
        return result;
    }

    public static StrippedPartition getStrippedPartition(AttributeSet attributeSet, DataFrame data, boolean isSample) {
        Map<AttributeSet, StrippedPartition> cachePtr;
        if (isSample)
            cachePtr = sampleCache;
        else
            cachePtr = cache;
        if (cachePtr.containsKey(attributeSet)) {

            return cachePtr.get(attributeSet);
        }
        StrippedPartition result = null;
        for (int attribute : attributeSet) {
            AttributeSet oneLess = attributeSet.deleteAttribute(attribute);
            if (cachePtr.containsKey(oneLess)) {
                result = cachePtr.get(oneLess).deepClone().product(attribute);
            }
        }
        if (result == null) {
            result = new StrippedPartition(data);
            for (int attribute : attributeSet) {
                result.product(attribute);
            }
        }
        if (isSample) {
            synchronized (sampleCache) {
                cachePtr.put(attributeSet, result);
            }
        } else {
            synchronized (cache) {
                cachePtr.put(attributeSet, result);
            }
        }

        return result;

    }

    public long splitRemoveCount(int right) {
        Timer timer = new Timer();
        long result = 0;
        for (int beginPointer = 0; beginPointer < begins.size() - 1; beginPointer++) {

            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);
            int groupLength = groupEnd - groupBegin;
            HashMap<Integer, Integer> groupInt2count = new HashMap<>();

            for (int i = groupBegin; i < groupEnd; i++) {
                int rightValue = data.get(indexes.get(i), right);
                groupInt2count.put(rightValue, groupInt2count.getOrDefault(rightValue, 0) + 1);
            }
            int max = Integer.MIN_VALUE;
            for (int count : groupInt2count.values()) {
                max = Math.max(max, count);
            }
            result += groupLength - max;
        }
        validateTime += timer.getTimeUsed();
        return result;
    }

    public long swapRemoveCount(SingleAttributePredicate left, int right) {
        System.out.println("swapRemoveCount");
        System.out.println(this);
        int length = indexes.size();
        int[] vioCount = new int[length];
        boolean[] deleted = new boolean[length];
        int result = 0;
        nextClass: for (int beginPointer = 0; beginPointer < begins.size() - 1; beginPointer++) {
            int groupBegin = begins.get(beginPointer);
            int groupEnd = begins.get(beginPointer + 1);
            for (int i = groupBegin; i < groupEnd; i++) {
                int lefti = filteredDataFrameGet(data, indexes.get(i), left);
                int righti = data.get(indexes.get(i), right);
                for (int j = i + 1; j < groupEnd; j++) {
                    int diffLeft = lefti - filteredDataFrameGet(data, indexes.get(j), left);
                    int diffRight = righti - data.get(indexes.get(j), right);
                    if (diffLeft != 0 && diffRight != 0 && (diffLeft > 0 != diffRight > 0)) {
                        vioCount[i]++;
                        vioCount[j]++;
                    }
                }
            }
            while (true) {
                int deleteIndex = -1;
                for (int i = groupBegin; i < groupEnd; i++) {
                    if (!deleted[i] && (deleteIndex == -1 || vioCount[i] > vioCount[deleteIndex])) {
                        deleteIndex = i;
                    }
                }
                if (deleteIndex == -1 || vioCount[deleteIndex] == 0) {
                    continue nextClass;
                }
                result++;
                deleted[deleteIndex] = true;
                int leftj = filteredDataFrameGet(data, indexes.get(deleteIndex), left);
                int rightj = data.get(indexes.get(deleteIndex), right);
                for (int i = groupBegin; i < groupEnd; i++) {
                    int diffLeft = leftj - filteredDataFrameGet(data, indexes.get(i), left);
                    int diffRight = rightj - data.get(indexes.get(i), right);
                    if (diffLeft != 0 && diffRight != 0 && (diffLeft > 0 != diffRight > 0)) {
                        vioCount[i]--;
                    }
                }
            }
        }
        return result;
    }

    public long swapRemoveCountEDBT(SingleAttributePredicate left, int right) {
        System.out.println("swapRemoveCountEDBT");

        List<Integer> sortedIndex = new ArrayList<>(indexes);

        List<Integer> sortedBegin = new ArrayList<>(begins);
        for (int beginPointer = 0; beginPointer < sortedBegin.size() - 1; beginPointer++) {
            int groupBegin = sortedBegin.get(beginPointer);
            int groupEnd = sortedBegin.get(beginPointer + 1);
            List<Integer> part = sortedIndex.subList(groupBegin, groupEnd);
            part.sort((A, B) -> {
                if (data.get(A, left.attribute) == data.get(B, left.attribute))
                    return data.get(A, right) - data.get(B, right);
                else
                    return data.get(A, left.attribute) - data.get(B, left.attribute);
            });
        }

        return data.getTupleCount() - lengthOfLISBS(sortedIndex, right);
    }

    public long lengthOfLIS(List<Integer> index, int right) {
        System.out.println("lengthOfLIS");
        int n = index.size();
        int[] dp = new int[n];
        int[] nums = new int[n];
        for (int i = 0; i < n; i++) {
            nums[i] = data.get(index.get(i), right);
            System.out.print(nums[i] + " ");
        }
        System.out.println();
        int res = 0;
        Arrays.fill(dp, 1);
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < i; j++) {
                if (nums[j] <= nums[i])
                    dp[i] = Math.max(dp[i], dp[j] + 1);
            }
            res = Math.max(res, dp[i]);
        }
        System.out.println(res);
        return res;
    }

    public long lengthOfLISBS(List<Integer> index, int right) {
        System.out.println("lengthOfLISBS");
        int n = index.size();
        int[] tails = new int[n];
        int[] nums = new int[n];
        for (int i = 0; i < n; i++) {
            nums[i] = data.get(index.get(i), right);

        }
        int res = 0;
        System.out.println();
        for (int num : nums) {
            int i = 0, j = res;
            while (i < j) {
                int m = (i + j) / 2;
                if (tails[m] <= num)
                    i = m + 1;
                else
                    j = m;
            }
            tails[i] = num;
            if (res == j)
                res++;
        }
        System.out.println(res);
        return res;
    }

    private int filteredDataFrameGet(DataFrame data, int tuple, SingleAttributePredicate column) {
        int result = data.get(tuple, column.attribute);

        if (column.operator == Operator.GREATEREQUAL) {
            result = -result;
        }
        return result;
    }

    public void clearCache(boolean isSample) {
        if (isSample) {
            sampleCache.clear();
        } else
            cache.clear();
    }

    public boolean isUCC() {
        return this.begins.size() == this.indexes.size() + 1;
    }

    public int trueContextLength(int index1, int index2, int right) {
        int l = 0;
        List<Integer> a = data.getTuple(index1);
        List<Integer> b = data.getTuple(index2);
        for (int i = 0; i < data.getColumnCount(); i++) {
            if (i != right)
                if (Objects.equals(a.get(i), b.get(i)))
                    l++;
        }
        return l;
    }

    public int trueContextLength(int index1, int index2, int right, int left) {
        int l = 0;
        List<Integer> a = data.getTuple(index1);
        List<Integer> b = data.getTuple(index2);
        for (int i = 0; i < data.getColumnCount(); i++) {
            if (i != right && i != left)
                if (Objects.equals(a.get(i), b.get(i)))
                    l++;
        }
        return l;
    }

    public static int lengthOfLIS(List<Integer> nums) {
        int[] tails = new int[nums.size()];
        int res = 0;
        for (int num : nums) {
            int i = 0, j = res;
            while (i < j) {
                int m = (i + j) / 2;
                if (tails[m] <= num)
                    i = m + 1;
                else
                    j = m;
            }
            tails[i] = num;
            if (res == j)
                res++;
        }
        return res;
    }

    // public static void main(String[] args) {
    //     DataFrame data = DataFrame.fromCsv("data/test1.csv");
    //     StrippedPartition sp = new StrippedPartition(data);
    //     System.out.println(sp);
    //     System.out.println(sp.isUCC());

    // }
}
