package fastod;

import dependencyDiscover.Data.DataFrame;
import dependencyDiscover.Dependency.LexicographicalOrderDependency;
import dependencyDiscover.Predicate.Operator;
import dependencyDiscover.Predicate.SingleAttributePredicate;
import util.Gateway;
import util.Util;
import util.Timer;

import java.util.*;

public class FasTOD {

    private final long timeLimit;
    private boolean complete = true;

    // 为得到的setOD生成对应的哈希表，用于转换成LOD
    Map<String, Integer> fdHashMap = new HashMap<>();
    Map<String, Integer> ocdHashMap = new HashMap<>();

    // M
    private List<CanonicalOD> result;
    // L
    private List<Set<AttributeSet>> contextInEachLevel;
    // cc
    private HashMap<AttributeSet, AttributeSet> cc;
    // cs
    private HashMap<AttributeSet, Set<AttributePair>> cs;
    // l
    // 设置成全局变量就可以实时的知道目前在第几层
    private int level;
    // R
    private AttributeSet schema;

    private DataFrame data;

    private double errorRateThreshold = -1f;

    Gateway traversalGateway;

    // statistics
    int odcount = 0, fdcount = 0, ocdcount = 0;

    Timer timer;

    void printStatistics() {
        if (traversalGateway.isOpen()) {
            Util.out(String.format("当前时间%.3f秒，发现od%d个，其中fd%d个，ocd%d个,最后一个od是%s", timer.getTimeUsedInSecond(),
                    fdcount + ocdcount, fdcount, ocdcount, result.size() > 0 ? result.get(result.size() - 1) : ""));
        }
    }

    public FasTOD(long timeLimit, double errorRateThreshold) {
        this.timeLimit = timeLimit;
        this.errorRateThreshold = errorRateThreshold;
    }

    public FasTOD(long timeLimit) {
        this.timeLimit = timeLimit;
    }

    private boolean timeUp() {
        return timer.getTimeUsed() >= timeLimit;
    }

    /**
     * 通过暴力穷举得到所有可能的ListOD
     * 使用元素的index
     * 
     * @return
     */
    private List<List<AttributeList>> generateCandidateSet() {
        // 初始化
        int level = 1;
        List<List<AttributeList>> listInEachLevel = new ArrayList<>();// 存每层的list
        List<AttributeList> thisLevel = new ArrayList<>();
        for (int i = 0; i < data.getColumnCount(); i++) {
            thisLevel.add(new AttributeList(i));
        }
        listInEachLevel.add(thisLevel);
        // 下一层需要本层的数据
        level++;
        while (level <= data.getColumnCount()) {
            Map<AttributeList, List<Integer>> prefixHashMap = new HashMap<>();// 用于记录该层中同一前缀对应的不同后缀
            List<AttributeList> preLevel = listInEachLevel.get(level - 2);
            thisLevel = new ArrayList<>(); // 初始化用来存储本层的候选者
            for (AttributeList att : preLevel) {
                AttributeList prefix = att.getPrefix();
                Integer last = att.getLast();
                if (!prefixHashMap.containsKey(prefix)) {
                    prefixHashMap.put(prefix, new ArrayList<>());
                }
                prefixHashMap.get(prefix).add(last);
            }
            for (Map.Entry<AttributeList, List<Integer>> entry : prefixHashMap.entrySet()) {
                AttributeList prefixTemp = entry.getKey();
                List<Integer> lastList = entry.getValue();
                for (int i = 0; i < lastList.size(); i++) {
                    for (int j = 0; j < lastList.size(); j++) {
                        if (i == j)
                            continue;
                        thisLevel.add(new AttributeList(prefixTemp, lastList.get(i), lastList.get(j)));
                    }
                }
            }
            listInEachLevel.add(thisLevel);
            level++;
        }
        return listInEachLevel;
    }

    /**
     * change set-basedOD to listed-basedOD
     * 判断时先考虑单方向的。即对OCD进行筛查，把两边方向不行等的去掉即可（FD其实没有方向的概念，因为FD只要保证无split即可）
     * 最朴素的算法，穷举所有可能的LOD并验证
     *
     */
    private List<LexicographicalOrderDependency> setOD2ListOD(List<CanonicalOD> setbasedOdResult) {
        List<LexicographicalOrderDependency> result = new ArrayList<>();
        List<List<AttributeList>> candidateInEachLevel = generateCandidateSet();

        // 为setBasedOD设置索引
        this.fdHashMap = new HashMap<>();
        this.ocdHashMap = new HashMap<>();
        for (CanonicalOD od : setbasedOdResult) {
            if (od.left == null) {
                this.fdHashMap.put(od.toHashString(), 1);
            } else if (od.left.operator == Operator.lessEqual) {
                this.ocdHashMap.put(od.toHashString(), 1);
            }
        }

        // 从第二层开始逐层遍历判断
        for (int i = 1; i < candidateInEachLevel.size(); i++) {
            List<AttributeList> thisLevel = candidateInEachLevel.get(i);
            for (AttributeList al : thisLevel) {
                result.addAll(generateListOD(al));
            }
        }

        return result;
    }

    /**
     * 通过把属性序列拆分成左右两部分，来去判断是否能构成LOD
     * 
     * @param at 一个属性序列，检查该序列可以得到多少对应的LOD
     * @return
     */
    private List<LexicographicalOrderDependency> generateListOD(AttributeList at) {
        List<LexicographicalOrderDependency> result = new ArrayList<>();
        int count = at.getAttributeCount();
        for (int i = 0; i < count - 1; i++) {
            String left = at.getSubString(0, i + 1);
            String right = at.getSubString(i + 1, at.getListLength());
            if (checkValid(left, right)) {
                // 将该LOD正式转换为LexicographicalOrderDependency
                LexicographicalOrderDependency lod = new LexicographicalOrderDependency(left, right);
                result.add(lod);
            }
        }
        return result;
    }

    /**
     * 给定一个LOD候选者，判断是否为有效LOD
     * 
     * @return
     */
    private boolean checkValid(String left, String right) {
        int leftLength = left.length();
        int rightLength = right.length();
        // 先检查FD
        for (int i = 0; i < rightLength; i++) {
            if (!fdHashMap.containsKey(left + right.charAt(i)))
                return false;
        }
        // 再检查OD
        for (int i = 0; i < leftLength; i++) {
            for (int j = 0; j < rightLength; j++) {
                if (!ocdHashMap
                        .containsKey(left.substring(0, i) + right.substring(0, j) + left.charAt(i) + right.charAt(j))) {
                    return false;
                }
            }
        }
        return true;
    }

    private void ccPut(AttributeSet key, AttributeSet attributeSet) {
        if (!cc.containsKey(key))
            cc.put(key, new AttributeSet());
        cc.put(key, attributeSet);
    }

    private void ccUnion(AttributeSet key, AttributeSet attributeSet) {
        if (!cc.containsKey(key))
            cc.put(key, new AttributeSet());
        cc.put(key, cc.get(key).union(attributeSet));
    }

    private void ccPut(AttributeSet key, int attribute) {
        if (!cc.containsKey(key))
            cc.put(key, new AttributeSet());
        cc.put(key, cc.get(key).addAttribute(attribute));
    }

    // 就是哈希表的取操作，没有就声明一个空value
    private AttributeSet ccGet(AttributeSet key) {
        if (!cc.containsKey(key))
            cc.put(key, new AttributeSet());
        return cc.get(key);
    }

    private void csPut(AttributeSet key, AttributePair value) {
        if (!cs.containsKey(key))
            cs.put(key, new HashSet<>());
        cs.get(key).add(value);
    }

    // 就是哈希表的取操作，没有就声明一个空value
    private Set<AttributePair> csGet(AttributeSet key) {
        if (!cs.containsKey(key))
            cs.put(key, new HashSet<>());
        return cs.get(key);
    }

    public boolean isComplete() {
        return complete;
    }

    // 初始化工作
    private void initialize(DataFrame data) {
        traversalGateway = new Gateway.ComplexTimeGateway();
        timer = new Timer();
        this.data = data;
        result = new ArrayList<>();
        // 两个集合，分别装FD和OCD
        cc = new HashMap<>();
        cs = new HashMap<>();

        contextInEachLevel = new ArrayList<>();
        contextInEachLevel.add(new HashSet<>());
        AttributeSet emptySet = new AttributeSet();
        // 对于这行代码我有疑问,我认为是这样的
        // 相当于在level0中增加了一个空set
        contextInEachLevel.get(0).add(emptySet);

        // 这里涉及到schema的初始化，schema就是R，所以几列就循环几次
        schema = new AttributeSet();
        for (int i = 0; i < data.getColumnCount(); i++) {
            schema = schema.addAttribute(i);
            // Cc({}) = R
            // 建立的映射是这样的 {} -> {A,B,C,...}
            ccPut(emptySet, i);
        }

        level = 1;

        HashSet<AttributeSet> level1Candidates = new HashSet<>();
        for (int i = 0; i < data.getColumnCount(); i++) {
            AttributeSet singleAttribute = emptySet.addAttribute(i);
            level1Candidates.add(singleAttribute);
        }

        contextInEachLevel.add(level1Candidates);
    }

    // 函数入口
    public List<CanonicalOD> discover(DataFrame data) {

        // initialize(data);
        while (contextInEachLevel.get(level).size() != 0) {
            Util.out(String.format("第%d层开始", level));
            computeODs();
            // 使用已用时间和设定时间对比判断是否超时
            if (timeUp()) {
                break;
            }
            pruneLevels();
            calculateNextLevel();
            if (timeUp()) {
                break;
            }
            level++;
        }
        if (isComplete()) {
            Util.out("FastOD算法正常结束");
        } else {
            Util.out("FastOD算法结束,运行超时");
        }
        Util.out(String.format("当前时间%.3f秒，发现od%d个，其中fd%d个，ocd%d个", timer.getTimeUsedInSecond(), fdcount + ocdcount,
                fdcount, ocdcount));
        return result;
    }

    private void computeODs() {
        // 得到当前level的attrSet集合
        Set<AttributeSet> contextThisLevel = contextInEachLevel.get(level);
        // 每次一个元素set
        for (AttributeSet context : contextThisLevel) {
            if (timeUp()) {
                complete = false;
                return;
            }
            AttributeSet contextCC = schema;
            // 遍历set中的attr
            for (int attribute : context) {
                contextCC = contextCC.intersect(ccGet(context.deleteAttribute(attribute)));
            }
            ccPut(context, contextCC);
            if (level == 2) {
                for (int i = 0; i < data.getColumnCount(); i++) {
                    for (int j = 0; j < data.getColumnCount(); j++) {
                        if (i == j)
                            continue;
                        AttributeSet c = new AttributeSet(Arrays.asList(i, j));
                        // SingleAttributePredicate相当于是封装了一下左侧的排序和谓词
                        csPut(c, new AttributePair(SingleAttributePredicate.getInstance(i, Operator.greaterEqual), j));
                        csPut(c, new AttributePair(SingleAttributePredicate.getInstance(i, Operator.lessEqual), j));
                    }
                }
            } else if (level > 2) {
                Set<AttributePair> candidateCsPairSet = new HashSet<>();
                for (int attribute : context) {
                    // 排除掉其中一个X\{C}得到的attributePair
                    candidateCsPairSet.addAll(csGet(context.deleteAttribute(attribute)));
                }
                for (AttributePair attributePair : candidateCsPairSet) {
                    // context删掉X\{A,B,C}
                    AttributeSet contextDeleteAB = context
                            .deleteAttribute(attributePair.left.attribute)
                            .deleteAttribute(attributePair.right);
                    boolean addContext = true;
                    for (int attribute : contextDeleteAB) {
                        if (!csGet(context.deleteAttribute(attribute)).contains(attributePair)) {
                            addContext = false;
                            break;
                        }
                    }
                    if (addContext) {
                        csPut(context, attributePair);
                    }
                }
            }
        }

        for (AttributeSet context : contextThisLevel) {
            if (timeUp()) {
                complete = false;
                return;
            }
            AttributeSet contextIntersectCCContext = context.intersect(ccGet(context));
            for (int attribute : contextIntersectCCContext) {
                CanonicalOD od = new CanonicalOD(context.deleteAttribute(attribute), attribute);
                // 验证OD
                if (od.isValid(data, errorRateThreshold)) {
                    result.add(od);
                    fdcount++;
                    // Remove A from Cc+(X)
                    ccPut(context, ccGet(context).deleteAttribute(attribute));
                    // Remove all B属于R\X from Cc+(X)
                    ccGet(context);
                    for (int i : schema.difference(context)) {
                        ccPut(context, ccGet(context).deleteAttribute(i));
                    }
                    printStatistics();
                }
            }
            List<AttributePair> attributePairsToRemove = new ArrayList<>();
            for (AttributePair attributePair : csGet(context)) {
                int a = attributePair.left.attribute;
                int b = attributePair.right;
                if (!ccGet(context.deleteAttribute(b)).containAttribute(a)
                        || !ccGet(context.deleteAttribute(a)).containAttribute(b)) {
                    attributePairsToRemove.add(attributePair);
                } else {
                    CanonicalOD od = new CanonicalOD(context.deleteAttribute(a).deleteAttribute(b),
                            attributePair.left, b);
                    if (od.isValid(data, errorRateThreshold)) {
                        ocdcount++;
                        result.add(od);
                        attributePairsToRemove.add(attributePair);
                    }
                    printStatistics();
                }

            }
            for (AttributePair attributePair : attributePairsToRemove) {
                csGet(context).remove(attributePair);
            }
        }
    }

    private void pruneLevels() {
        if (level >= 2) {
            List<AttributeSet> nodesToRemove = new ArrayList<>();
            for (AttributeSet attributeSet : contextInEachLevel.get(level)) {
                if (ccGet(attributeSet).isEmpty()
                        && csGet(attributeSet).isEmpty()) {
                    nodesToRemove.add(attributeSet);
                }
            }
            Set<AttributeSet> contexts = contextInEachLevel.get(level);
            for (AttributeSet attributeSet : nodesToRemove) {
                contexts.remove(attributeSet);
            }
        }
    }

    private void calculateNextLevel() {
        Map<AttributeSet, List<Integer>> prefixBlocks = new HashMap<>();
        Set<AttributeSet> contextNextLevel = new HashSet<>();
        Set<AttributeSet> contextThisLevel = contextInEachLevel.get(level);

        // 去掉一个元素，得到前缀放入哈希表
        for (AttributeSet attributeSet : contextThisLevel) {
            for (Integer attribute : attributeSet) {
                AttributeSet prefix = attributeSet.deleteAttribute(attribute);
                if (!prefixBlocks.containsKey(prefix)) {
                    prefixBlocks.put(prefix, new ArrayList<>());
                }
                prefixBlocks.get(prefix).add(attribute);
            }
        }

        for (Map.Entry<AttributeSet, List<Integer>> attributeSetListEntry : prefixBlocks.entrySet()) {
            if (timeUp()) {
                complete = false;
                return;
            }
            AttributeSet prefix = attributeSetListEntry.getKey();
            List<Integer> singleAttributes = attributeSetListEntry.getValue();
            // 这种情况不满足
            if (singleAttributes.size() <= 1)
                continue;
            // 两两组合
            for (int i = 0; i < singleAttributes.size(); i++) {
                for (int j = i + 1; j < singleAttributes.size(); j++) {
                    boolean createContext = true;
                    // X=Y∪{B,C}
                    AttributeSet candidate = prefix.addAttribute(singleAttributes.get(i))
                            .addAttribute(singleAttributes.get(j));
                    // 判断是否有∀A∈X X\A ∈ L
                    for (int attribute : candidate) {
                        if (!contextThisLevel.contains(candidate.deleteAttribute(attribute))) {
                            createContext = false;
                            break;
                        }
                    }
                    if (createContext) {
                        contextNextLevel.add(candidate);
                    }
                }
            }
        }
        contextInEachLevel.add(contextNextLevel);
    }

    public static void main(String[] args) {
        // DataFrame data = DataFrame.fromCsv("./data/exp1/ATOM/Atom 10.csv");
        DataFrame data = DataFrame.fromCsv("./data/test2.csv");
        FasTOD f = new FasTOD(1000000, -1f);
        f.initialize(data);
        List<CanonicalOD> result = f.discover(data);
        System.out.println(result);
        System.out.println(f.setOD2ListOD(result));
        System.exit(0);
    }
}
