package fastod_mt;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import dependencyDiscover.Data.DataFrame;
import dependencyDiscover.Predicate.Operator;
import dependencyDiscover.Predicate.SingleAttributePredicate;
import fastod.AttributePair;
import fastod.AttributeSet;
import fastod.CanonicalOD;
import org.checkerframework.checker.units.qual.A;
import org.checkerframework.checker.units.qual.C;
import util.Gateway;
import util.Timer;
import util.Util;

import java.security.PrivateKey;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FastOD_mt {

    private final long timeLimit;
    private boolean complete = true;

    private final ThreadFactory namedThreadFactory;

    //  不允许使用Executors创建线程池，防止OOM
    private final ThreadPoolExecutor pool;


    // 为得到的setOD生成对应的哈希表，用于转换成LOD
    Map<String, Integer> fdHashMap = new ConcurrentHashMap<>();
    Map<String, Integer> ocdHashMap = new ConcurrentHashMap<>();

    //分别记录一个context下，fd，ocd，整体是否被验证过，当两个都为0时，可以对该node进行prune
    Map<AttributeSet, AtomicInteger> CcCount = new ConcurrentHashMap<>();
    Map<AttributeSet, AtomicInteger> CsCount = new ConcurrentHashMap<>();
    //记录context是否被prune过
    Map<AttributeSet, Boolean> isPrune = new ConcurrentHashMap<>();

    //用来确保所有线程完成工作后，打印结果
    CountDownLatch latch;

//    Map<String, Integer> fdHashMap = new ConcurrentHashMap<>();
    // M
    private List<CanonicalOD> result;
    // L 每层候选集
    //List 表示每层，Set 表示候选的集合，集合中的元素是attributeSet
    private List<Set<AttributeSet>> contextInEachLevel;
    // cc Key是context，Value是 []->right 中的right元素集合
    private ConcurrentMap<AttributeSet, AttributeSet> cc;
    // cs
    private ConcurrentMap<AttributeSet, Set<AttributePair>> cs;
    // l
    // 设置成全局变量就可以实时的知道目前在第几层，不需要共享，由main使用即可
    private int level;
    // R
    private AttributeSet schema;

    private double errorRateThreshold = -1f;

    Gateway traversalGateway;

    // statistics
    AtomicInteger odcount,fdcount,ocdcount;

    private DataFrame data;

    Timer timer;

    void printStatistics() {
        if (traversalGateway.isOpen()) {
            Util.out(String.format("当前时间%.3f秒，发现od%d个，其中fd%d个，ocd%d个,最后一个od是%s", timer.getTimeUsedInSecond(),
                    fdcount.intValue() + ocdcount.intValue(), fdcount.intValue(), ocdcount.intValue(), result.size() > 0 ? result.get(result.size() - 1) : ""));
        }
    }

    public FastOD_mt(long timeLimit, double errorRateThreshold) {
        this.timeLimit = timeLimit;
        this.errorRateThreshold = errorRateThreshold;

        //创建线程池
        namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("pool-%d").build();
        pool = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
    }

    private boolean timeUp() {
        return timer.getTimeUsed() >= timeLimit;
    }

    /**
     * 在Cc+中放入一个值
     * @param key 本层的某个context
     * @param attributeSet 满足条件的Attribute集合
     */
    private void ccPut(AttributeSet key, AttributeSet attributeSet) {
        if (!cc.containsKey(key))
            cc.put(key, new AttributeSet());
        cc.put(key, attributeSet);
    }

    /**
     * 在Cc+中放入一个值
     * @param key 本层中的context
     * @param attribute int表示的单一元素
     */
    private void ccPut(AttributeSet key, int attribute) {
        if (!cc.containsKey(key))
            cc.put(key, new AttributeSet());
        cc.put(key, cc.get(key).addAttribute(attribute));
    }

    /**
     * 集合交操作
     * @param key 本层的某个context
     * @param attributeSet 满足条件的Attribute集合
     */
    private void ccUnion(AttributeSet key, AttributeSet attributeSet) {
        if (!cc.containsKey(key))
            cc.put(key, new AttributeSet());
        cc.put(key, cc.get(key).union(attributeSet));
    }

    /**
     * 读取Cc+
     * @param key 某个context
     * @return 返回该context下可能成为最小OD的集合
     */
    private AttributeSet ccGet(AttributeSet key) {
        if (!cc.containsKey(key))
            cc.put(key, new AttributeSet());
        return cc.get(key);
    }

    /**
     * 构建Cs集合
     * @param key 某个context
     * @param value A~B形式
     */
    private void csPut(AttributeSet key, AttributePair value) {
        if (!cs.containsKey(key))
            cs.put(key, new HashSet<>());
        cs.get(key).add(value);
    }

    /**
     * 获得Cs集合
     * @param key 某个context
     * @return 获取该context下的可能成为最小的OD集合
     */
    private Set<AttributePair> csGet(AttributeSet key) {
        if (!cs.containsKey(key))
            cs.put(key, new HashSet<>());
        return cs.get(key);
    }

    public boolean isComplete() {
        pool.shutdown();
        while (!pool.isTerminated());
        return complete;
    }

    /**
     * 初始化fastOD需要的数据结构
     * 生成第0层和第1层的候选集
     * 生成 Cc({}) = R
     * @param data 数据集
     */
    private void initialize(DataFrame data) {
        traversalGateway = new Gateway.ComplexTimeGateway();
        timer = new Timer();
        this.data = data;

        result = Collections.synchronizedList(new ArrayList<>());

        // 两个集合，分别装FD和OCD
        cc = new ConcurrentHashMap<>();
        cs = new ConcurrentHashMap<>();

        fdcount = new AtomicInteger(0);
        ocdcount = new AtomicInteger(0);
        //每一层记录一个
        contextInEachLevel = Collections.synchronizedList(new ArrayList<>());

        //建立第0层的hashset
        //TODO 选择一个合理的容器（里面可以是hashset，因为不需要考虑并发访问）
        contextInEachLevel.add(new HashSet<>());
        AttributeSet emptySet = new AttributeSet();
        // （对于这行代码我有疑问,我认为是这样的）
        // 相当于在level0中增加了一个空set
        contextInEachLevel.get(0).add(emptySet);

        // 这里涉及到schema的初始化，schema就是R，把所有列都加入Schema
        schema = new AttributeSet();
        for (int i = 0; i < data.getColumnCount(); i++) {
            schema = schema.addAttribute(i);
            // Cc({}) = R
            // 建立的映射是这样的 {} -> {A,B,C,...}
            ccPut(emptySet, i);
        }

        level = 1;

        HashSet<AttributeSet> level1Candidates = new HashSet<>();
        //遍历每一列，每次加一个列
        for (int i = 0; i < data.getColumnCount(); i++) {
            AttributeSet singleAttribute = emptySet.addAttribute(i);
            level1Candidates.add(singleAttribute);
        }

        contextInEachLevel.add(level1Candidates);
    }

    /**
     * SOD发现算法
     * @param data 数据集
     * @return 经典SOD列表
     */
    public List<CanonicalOD> discover(DataFrame data) {
        //从第0层开始，如果候选集大小不为0，则可以进行验证
        while (contextInEachLevel.get(level).size() != 0) {
            Util.out(String.format("第%d层开始，需要prune%d个context", level,contextInEachLevel.get(level).size()));
            latch = new CountDownLatch(contextInEachLevel.get(level).size());
            computeODs();
            // 使用已用时间和设定时间对比判断是否超时
            if (timeUp()) {
                break;
            }
            try{
                latch.await();
                System.out.println(String.format("level%d完成",level));
            }
            catch (Exception e){
                System.out.println("出现错误");
            }
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
        Util.out(String.format("当前时间%.3f秒，发现od%d个，其中fd%d个，ocd%d个", timer.getTimeUsedInSecond(), fdcount.intValue() + ocdcount.intValue(),
                fdcount.intValue(), ocdcount.intValue()));
        return result;
    }

    /**
     * 计算本层SOD，因为cc，cs，都是全局变量，所以没有参数
     */
    private void computeODs() {
        // 得到当前level的attrSet集合
        Set<AttributeSet> contextThisLevel = contextInEachLevel.get(level);
        // 每次取出一个context
        for (AttributeSet context : contextThisLevel) {
            if (timeUp()) {
                complete = false;
                return;
            }
            AttributeSet contextCC = schema;
            // 生成Cc
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
                    // 取并，排除掉其中一个X\{C}得到的attributePair
                    candidateCsPairSet.addAll(csGet(context.deleteAttribute(attribute)));
                }
                for (AttributePair attributePair : candidateCsPairSet) {
                    // context删掉X\{A,B,C}
                    AttributeSet contextDeleteAB = context
                            .deleteAttribute(attributePair.left.attribute)
                            .deleteAttribute(attributePair.right);
                    boolean addContext = true;
                    //context/{A、B、D}
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

        //避免ConcurrentModificationException
        Set<AttributeSet> temp = new HashSet<>(contextThisLevel);
        for (AttributeSet context : temp) {
            if (timeUp()) {
                complete = false;
                return;
            }
            AttributeSet contextIntersectCCContext = context.intersect(ccGet(context));
            CcCount.put(context,new AtomicInteger(contextIntersectCCContext.getSize()));
            //TODO 算出该context需要计算几个Cc和几个Cs，当所有都做完后，可以进行prune步骤
            for (int attribute : contextIntersectCCContext) {
                pool.submit(new ValidateFD(context,attribute));
            }
            CsCount.put(context,new AtomicInteger(csGet(context).size()));
            for (AttributePair attributePair : csGet(context)) {
                pool.submit(new ValidateOCD(context,attributePair));
            }
        }
    }

    /**
     * 如果本层Cc和Cs均为空，则该节点可以被删除
     */
    private void pruneLevels(AttributeSet context) {
        System.out.println(Integer.toBinaryString(context.getValue())+"已经被prune");
        System.out.println(latch.getCount());
        int level = context.getSize();
        if (level >= 2) {
            Set<AttributeSet> contexts = contextInEachLevel.get(level);
            if (ccGet(context).isEmpty() && csGet(context).isEmpty()){
                contexts.remove(context);
            }
        }
        isPrune.put(context,Boolean.TRUE);
        latch.countDown();
    }


    /**
     *
     * @param context
     * @param ccOrcs 1 cc 2 cs
     * @return
     */
    synchronized public boolean canPrune(AttributeSet context,int ccOrcs){
        if(ccOrcs==1)
            CcCount.get(context).decrementAndGet();
        else
            CsCount.get(context).decrementAndGet();
        return CcCount.get(context).intValue() == 0 && (level==1 || CsCount.get(context).intValue() == 0);
    }

    /**
     * 使用apriori算法生成下一层的Context
     * 若引入多线程，需要改写哪里？
     * TODO 就是prune的可能不及时，需要优先，先实现不优化的版本
     */
    private void calculateNextLevel() {
        //本层context，拆分成前缀+一个元素
        Map<AttributeSet, List<Integer>> prefixBlocks = new HashMap<>();
        Set<AttributeSet> contextNextLevel = new HashSet<>();
        Set<AttributeSet> contextThisLevel = contextInEachLevel.get(level);

        // 去掉一个元素，得到前缀放入哈希表
        for (AttributeSet attributeSet : contextThisLevel) {
            for (Integer attribute : attributeSet) {
                AttributeSet prefix = attributeSet.deleteAttribute(attribute);
                //把前缀相同的后缀放在一起
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

class ValidateFD implements Runnable{
        private final AttributeSet context;
        private final int attribute;

        public ValidateFD(AttributeSet context,int attribute) {
            this.context = context;
            this.attribute = attribute;
        }

        /**
         * 验证该是否成立
         * 这部分内容可以分派给一个线程完成，那么验证就变成了异步的过程
         * 此外还要判断本节点是否可以prune
         * context 上下文
         * attribute [] -> attribute
         */
        @Override
        public void run() {
            System.out.println(Thread.currentThread().getId()+"开始工作"+context.toString()+"进行验证"+attribute);
            //利用候选集构建OD
            CanonicalOD od = new CanonicalOD(context.deleteAttribute(attribute), attribute);

            //验证OD
            if (od.isValid(data, errorRateThreshold)) {
                result.add(od);
                fdcount.incrementAndGet();
                // Remove A from Cc+(X)
                ccPut(context, ccGet(context).deleteAttribute(attribute));
                // Remove all B属于R\X from Cc+(X)
                ccGet(context);
                for (int i : schema.difference(context)) {
                    ccPut(context, ccGet(context).deleteAttribute(i));
                }
                printStatistics();
            }
            if(canPrune(context,1))
                pruneLevels(context);
        }
    }

class ValidateOCD implements Runnable{
        private final AttributeSet context;
        private final AttributePair attributePair;

        ValidateOCD(AttributeSet context, AttributePair attributePair) {
            this.context = context;
            this.attributePair = attributePair;
        }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getId()+"开始工作"+context.toString()+"进行验证"+attributePair.toString());
        List<AttributePair> attributePairsToRemove = new ArrayList<>();

        int a = attributePair.left.attribute;
        int b = attributePair.right;
        if (!ccGet(context.deleteAttribute(b)).containAttribute(a)
                || !ccGet(context.deleteAttribute(a)).containAttribute(b)) {
            attributePairsToRemove.add(attributePair);
        } else {
            CanonicalOD od = new CanonicalOD(context.deleteAttribute(a).deleteAttribute(b),
                    attributePair.left, b);
            if (od.isValid(data, errorRateThreshold)) {
                ocdcount.incrementAndGet();
                result.add(od);
                attributePairsToRemove.add(attributePair);
            }
            printStatistics();
        }

        for (AttributePair attributePairToRemove : attributePairsToRemove) {
            csGet(context).remove(attributePairToRemove);
        }
        if(canPrune(context,2))
            pruneLevels(context);
    }
}


    public static void main(String[] args) {
        DataFrame data = DataFrame.fromCsv("./data/exp1/ATOM/Atom 11.csv");
//        DataFrame data = DataFrame.fromCsv("./data/test2.csv");
        FastOD_mt f = new FastOD_mt(1000000, -1f);
        f.initialize(data);
        List<CanonicalOD> result = f.discover(data);
        System.out.println(result);
//        System.out.println(f.setOD2ListOD(result));
        System.exit(0);
    }
}
