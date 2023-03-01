package HyOD;

import dependencyDiscover.Data.DataFrame;
import dependencyDiscover.Data.PartialDataFrame;
import dependencyDiscover.Dependency.LexicographicalOrderDependency;
import dependencyDiscover.Predicate.Operator;
import dependencyDiscover.Predicate.SingleAttributePredicate;
import dependencyDiscover.sampler.OneLevelCheckingSampler;
import dependencyDiscover.sampler.SampleConfig;
import util.Gateway;
import util.Timer;
import util.Util;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class HyODM{

        private boolean VALIDINT;
        private final long timeLimit;
        private boolean complete = false;
        private int POOLSIZE;
        private int tupleNum;
//        private final ConcurrentLinkedQueue<CanonicalOD> ODCandidates = new ConcurrentLinkedQueue<>();
        private ThreadPoolExecutor pool ;
        private long validTime,dicoverTime;

        // 为得到的setOD生成对应的哈希表，用于转换成LOD
        private Map<String, Integer> fdHashMap = new HashMap<>();
        private Map<String, Integer> ocdHashMap = new HashMap<>();

        // M
        private final CopyOnWriteArraySet<CanonicalOD> result = new CopyOnWriteArraySet<>();
        private final CopyOnWriteArraySet<CanonicalOD> nonod = new CopyOnWriteArraySet<>();
        // L 每层候选集
        //List 表示每层，Set 表示候选的集合，集合中的元素是attributeSet
        private List<Set<AttributeSet>> contextInEachLevel;
        // cc Key是context，Value是 []->right 中的right元素集合
        private ConcurrentHashMap<AttributeSet, AttributeSet> cc;
        // cs
        private ConcurrentHashMap<AttributeSet, HashSet<AttributePair>> cs;
        // l
        // 设置成全局变量就可以实时的知道目前在第几层
        private int level;
        // R
        private AttributeSet schema;

        private DataFrame data;
        private PartialDataFrame sampleData;
        private final CopyOnWriteArraySet<Integer> viorows = new CopyOnWriteArraySet<>();
        private double errorRateThreshold = -1f;

        Gateway traversalGateway;

        // statistics
        private final AtomicInteger fdcount = new AtomicInteger(0);
        private final AtomicInteger ocdcount = new AtomicInteger(0);
        util.Timer timer = new util.Timer();

//    void printStatistics() {
//        if (traversalGateway.isOpen()) {
//            Util.out(String.format("当前时间%.3f秒，发现od%d个，其中fd%d个，ocd%d个,最后一个od是%s", timer.getTimeUsedInSecond(),
//                    fdcount + ocdcount, fdcount, ocdcount, result.size() > 0 ? result.get(result.size() - 1) : ""));
//        }
//    }

        public HyODM(long timeLimit, double errorRateThreshold,int poolsize) {
            this.timeLimit = timeLimit;
            this.errorRateThreshold = errorRateThreshold;
            this.POOLSIZE = poolsize;
        }


        public HyODM(long timeLimit) {
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

        /**
         * 在Cc+中放入一个值
         *
         * @param key          本层的某个context
         * @param attributeSet 满足条件的Attribute集合
         */
        private void ccPut(AttributeSet key, AttributeSet attributeSet) {
            if (!cc.containsKey(key))
                cc.put(key, new AttributeSet());
            cc.put(key, attributeSet);
        }

        /**
         * 在Cc+中放入一个值
         *
         * @param key       本层中的context
         * @param attribute int表示的单一元素
         */
        private void ccPut(AttributeSet key, int attribute) {
            if (!cc.containsKey(key))
                cc.put(key, new AttributeSet());
            cc.put(key, cc.get(key).addAttribute(attribute));
        }

        /**
         * 集合交操作
         *
         * @param key          本层的某个context
         * @param attributeSet 满足条件的Attribute集合
         */
        private void ccUnion(AttributeSet key, AttributeSet attributeSet) {
            if (!cc.containsKey(key))
                cc.put(key, new AttributeSet());
            cc.put(key, cc.get(key).union(attributeSet));
        }

        /**
         * 读取Cc+
         *
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
         *
         * @param key   某个context
         * @param value A~B形式
         */
        private void csPut(AttributeSet key, AttributePair value) {
            if (!cs.containsKey(key))
                cs.put(key, new HashSet<>());
            cs.get(key).add(value);
        }

        /**
         * 获得Cs集合
         *
         * @param key 某个context
         * @return 获取该context下的可能成为最小的OD集合
         */
        private Set<AttributePair> csGet(AttributeSet key) {
            if (!cs.containsKey(key))
                cs.put(key, new HashSet<>());
            return cs.get(key);
        }

        /**
         * 根据最后一层od的存在判断该属性是否能作为FD的右侧
         *
         * @param attribute 被检验的属性
         * @return 能否作为FD的右侧
         */
        private boolean checkAttribute(int attribute) {
            StrippedPartition sp = new StrippedPartition(data);
            if(attribute !=0) {
                for (int i = 0; i < data.getColumnCount(); i++) {
                    if(i!=attribute)
                        sp.product(i);
                }
            }
            else{
                for (int i = 1; i < data.getColumnCount(); i++) {
                    sp.product(i);
                }
            }
            return !sp.split(attribute);
        }

        public boolean isComplete() {
            return complete;
        }

        /**
         * 初始化fastOD需要的数据结构
         * 生成第0层和第1层的候选集
         * 生成 Cc({}) = R
         *
         * @param data 数据集
         */
        private void initialize(DataFrame data,PartialDataFrame sampleData,int tupleNum) {
            traversalGateway = new Gateway.ComplexTimeGateway();

            this.data = data;
            this.VALIDINT = false;
            this.sampleData = sampleData;
            this.viorows.clear();
            this.pool = new ThreadPoolExecutor(POOLSIZE, POOLSIZE,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>());
            this.tupleNum = tupleNum;
            // 两个集合，分别装FD和OCD
            cc = new ConcurrentHashMap<>();
            cs = new ConcurrentHashMap<>();


            //每一层记录一个
            contextInEachLevel = new ArrayList<>();

            //建立第0层的hashset
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
//            if(!checkAttribute(i)){
//                vioAttribute.add(i);
//                System.out.println("Delete Attribute:"+i);
//            }
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

            //线程监听ODCandidates，查询是否存在任务
//            for(int i=0;i<POOLSIZE;i++){
//                pool.execute(()->{
//                    while(((!DISCOVERDONE)||(!ODCandidates.isEmpty()))&&!VALIDINT) {
//                        boolean valid = false;
//                        CanonicalOD od = null;
//                        synchronized (ODCandidates){
//                            if (!ODCandidates.isEmpty()) {
//                                od = ODCandidates.poll();
//                                valid = true;
//                            }
//                        }
//                        if(valid) {
//                            validateCandidate(od);
//                            finishedOD.getAndIncrement();
//                        }
//                    }
//                });
//            }
        }

        public void validateCandidate(CanonicalOD od){
            Set<Integer> addVio;
            addVio = od.validForSample(data,errorRateThreshold,false,10);
            if(addVio.isEmpty()){
                result.add(od);
                if(od.left == null){
                    AttributeSet context = od.context;
                    ccPut(context, ccGet(context).deleteAttribute(od.right));
                    // Remove all B属于R\X from Cc+(X)
                    ccGet(context);
                    for (int i : schema.difference(context)) {
                        ccPut(context, ccGet(context).deleteAttribute(i));
                    }
                    fdcount.getAndIncrement();
                }
                else{
                    ocdcount.getAndIncrement();
                }
            }
            else {
                viorows.addAll(addVio);
            }
        }

        /**
         * SOD发现算法
         *
         * @param data 数据集
         * @return 经典SOD列表
         */
        public Set<CanonicalOD> discover(DataFrame data) {
            //从第0层开始，如果候选集大小不为0，则可以进行验证
            while (contextInEachLevel.get(level).size() != 0) {
                Util.out(String.format("level %d start", level));
                computeODs();
//                if(viorows.size() > 5){
//                    System.out.println("VioSize:"+viorows.size());
//                    sampleData.addRowsFromOriginalDataFrame(viorows);
//                    new StrippedPartition().clearCache(true);
//                    viorows.clear();
//                break;
//                }
                while(pool.getActiveCount() != 0){
                    pool.getActiveCount();
                };
                sampleData.addRowsFromOriginalDataFrame(viorows);
                CanonicalOD.clearForResult(true);
                viorows.clear();
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
                Util.out("HyOD Finish");
            } else {
                Util.out("HyOD Time Limited");
            }
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

            for (AttributeSet context : contextThisLevel) {
                pool.execute(()->{
                    if (timeUp()) {
                        complete = false;
                        return;
                    }
                    AttributeSet contextIntersectCCContext = context.intersect(ccGet(context));
                    for (int attribute : contextIntersectCCContext) {
                        //利用候选集构建OD
                        CanonicalOD od = new CanonicalOD(context.deleteAttribute(attribute), attribute);
                        //验证OD
                            if (od.isValid(sampleData, errorRateThreshold,true)) {
                                long temp = timer.getTimeUsed();
                                Set<Integer>  addvio = od.validForSample(data, errorRateThreshold, false,tupleNum);
                                if (addvio.isEmpty()) {
                                    // Remove A from Cc+(X)
                                    ccPut(context, ccGet(context).deleteAttribute(attribute));
                                    // Remove all B属于R\X from Cc+(X)
                                    ccGet(context);
                                    for (int i : schema.difference(context)) {
                                        ccPut(context, ccGet(context).deleteAttribute(i));
                                    }
                                    result.add(od);
                                    fdcount.getAndIncrement();
                                } else {
                                    viorows.addAll(addvio);
                                }
                                validTime += timer.getTimeUsed() - temp;
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

                            boolean finished = false;
                            if (od.isValid(sampleData, errorRateThreshold,true)) {
                                long temp = timer.getTimeUsed();
                                Set<Integer> addvios = od.validForSample(data,errorRateThreshold,false,tupleNum);
                                if(addvios.isEmpty()) {
                                    attributePairsToRemove.add(attributePair);
                                    result.add(od);
                                    ocdcount.getAndIncrement();
                                }
                                else{
                                    viorows.addAll(addvios);
                                }
                                validTime += timer.getTimeUsed()-temp;
                            }
                        }

                    }
                    for (AttributePair attributePair : attributePairsToRemove) {
                        csGet(context).remove(attributePair);
                    }
                });

            }
        }


        /**
         * 如果本层Cc和Cs均为空，则该节点可以被删除
         */
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

        /**
         * 使用apriori算法生成下一层的Context
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

        public static void main(String[] args) {
            DataFrame data = DataFrame.fromCsv(args[0]);
            util.Timer spTime = new Timer();
            Runtime r = Runtime.getRuntime();
            OneLevelCheckingSampler sampler = new OneLevelCheckingSampler();
            SampleConfig config = new SampleConfig(Integer.parseInt(args[1]));
            PartialDataFrame sampleData = sampler.sample(data,config);
            System.out.println("|r'|：" + sampleData.getRowsCount());
            long sampleTime = spTime.getTimeUsed();

            HyODM f = new HyODM(30000000, -1f,Integer.parseInt(args[3]));

            f.initialize(data,sampleData,Integer.parseInt(args[2]));
//            f.pool.execute(()->{f.discover(sampleData);});
//            Set<CanonicalOD> result = f.result;
            Set<CanonicalOD> result = f.discover(sampleData);

//            while (true) {
//
//                f.initialize(data,sampleData);
//                result = f.discover(sampleData);
//                if (f.viorows.isEmpty()&&!f.VALIDINT) {
//                    System.out.println("viorows为空，算法结束");
//                    break;
//                }
//                else {
//                    CanonicalOD.clearForResult(true);
//                    sampleData.addRowsFromOriginalDataFrame(f.viorows);
//                }
//            }

            long startM = r.freeMemory();
            long endM = r.totalMemory();
            System.out.println("MemoryCost: " + String.valueOf((endM-startM)/1024/1024) + "MB");
            System.out.println(result);
//        System.out.println(f.setOD2ListOD(result));
            System.exit(0);
        }

    }

