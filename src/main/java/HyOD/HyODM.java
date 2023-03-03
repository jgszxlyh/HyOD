package hyod;

import util.Gateway;
import util.Timer;
import util.Util;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import dependencydiscover.dataframe.DataFrame;
import dependencydiscover.dataframe.PartialDataFrame;
import dependencydiscover.predicate.Operator;
import dependencydiscover.predicate.SingleAttributePredicate;
import dependencydiscover.sampler.OneLevelCheckingSampler;
import dependencydiscover.sampler.SampleConfig;

public class HyODM {

    private final long timeLimit;
    private boolean complete = false;
    private int POOLSIZE;
    private int tupleNum;

    public boolean isComplete() {
        return complete;
    }

    private ThreadPoolExecutor pool;
    private long validTime, dicoverTime;

    private final CopyOnWriteArraySet<CanonicalOD> result = new CopyOnWriteArraySet<>();

    private List<Set<AttributeSet>> contextInEachLevel;

    private ConcurrentHashMap<AttributeSet, AttributeSet> cc;

    private ConcurrentHashMap<AttributeSet, HashSet<AttributePair>> cs;

    private int level;

    private AttributeSet schema;

    private DataFrame data;
    private PartialDataFrame sampleData;
    private final CopyOnWriteArraySet<Integer> viorows = new CopyOnWriteArraySet<>();
    private double errorRateThreshold = -1f;

    Gateway traversalGateway;

    private final AtomicInteger fdcount = new AtomicInteger(0);
    private final AtomicInteger ocdcount = new AtomicInteger(0);
    util.Timer timer = new util.Timer();

    public HyODM(long timeLimit, double errorRateThreshold, int poolsize) {
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

    private void ccPut(AttributeSet key, AttributeSet attributeSet) {
        if (!cc.containsKey(key))
            cc.put(key, new AttributeSet());
        cc.put(key, attributeSet);
    }

    private void ccPut(AttributeSet key, int attribute) {
        if (!cc.containsKey(key))
            cc.put(key, new AttributeSet());
        cc.put(key, cc.get(key).addAttribute(attribute));
    }

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

    private Set<AttributePair> csGet(AttributeSet key) {
        if (!cs.containsKey(key))
            cs.put(key, new HashSet<>());
        return cs.get(key);
    }

    private void initialize(DataFrame data, PartialDataFrame sampleData, int tupleNum) {
        traversalGateway = new Gateway.ComplexTimeGateway();

        this.data = data;
        this.sampleData = sampleData;
        this.viorows.clear();
        this.pool = new ThreadPoolExecutor(POOLSIZE, POOLSIZE,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
        this.tupleNum = tupleNum;

        cc = new ConcurrentHashMap<>();
        cs = new ConcurrentHashMap<>();

        contextInEachLevel = new ArrayList<>();

        contextInEachLevel.add(new HashSet<>());
        AttributeSet emptySet = new AttributeSet();

        contextInEachLevel.get(0).add(emptySet);

        schema = new AttributeSet();
        for (int i = 0; i < data.getColumnCount(); i++) {
            schema = schema.addAttribute(i);

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

    public void validateCandidate(CanonicalOD od) {
        Set<Integer> addVio;
        addVio = od.validForSample(data, errorRateThreshold, false, 10);
        if (addVio.isEmpty()) {
            result.add(od);
            if (od.left == null) {
                AttributeSet context = od.context;
                ccPut(context, ccGet(context).deleteAttribute(od.right));

                ccGet(context);
                for (int i : schema.difference(context)) {
                    ccPut(context, ccGet(context).deleteAttribute(i));
                }
                fdcount.getAndIncrement();
            } else {
                ocdcount.getAndIncrement();
            }
        } else {
            viorows.addAll(addVio);
        }
    }

    public Set<CanonicalOD> discover(DataFrame data) {

        while (contextInEachLevel.get(level).size() != 0) {
            Util.out(String.format("level %d start", level));
            computeODs();

            while (pool.getActiveCount() != 0) {
                pool.getActiveCount();
            }
            ;
            sampleData.addRowsFromOriginalDataFrame(viorows);
            CanonicalOD.clearForResult(true);
            viorows.clear();

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

    private void computeODs() {

        Set<AttributeSet> contextThisLevel = contextInEachLevel.get(level);

        for (AttributeSet context : contextThisLevel) {
            if (timeUp()) {
                complete = false;
                return;
            }
            AttributeSet contextCC = schema;

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

                        csPut(c, new AttributePair(SingleAttributePredicate.getInstance(i, Operator.GREATEREQUAL), j));
                        csPut(c, new AttributePair(SingleAttributePredicate.getInstance(i, Operator.LESSEQUAL), j));
                    }
                }
            } else if (level > 2) {
                Set<AttributePair> candidateCsPairSet = new HashSet<>();
                for (int attribute : context) {

                    candidateCsPairSet.addAll(csGet(context.deleteAttribute(attribute)));
                }
                for (AttributePair attributePair : candidateCsPairSet) {

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
            pool.execute(() -> {
                if (timeUp()) {
                    complete = false;
                    return;
                }
                AttributeSet contextIntersectCCContext = context.intersect(ccGet(context));
                for (int attribute : contextIntersectCCContext) {

                    CanonicalOD od = new CanonicalOD(context.deleteAttribute(attribute), attribute);

                    if (od.isValid(sampleData, errorRateThreshold, true)) {
                        long temp = timer.getTimeUsed();
                        Set<Integer> addvio = od.validForSample(data, errorRateThreshold, false, tupleNum);
                        if (addvio.isEmpty()) {

                            ccPut(context, ccGet(context).deleteAttribute(attribute));

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
                        if (od.isValid(sampleData, errorRateThreshold, true)) {
                            long temp = timer.getTimeUsed();
                            Set<Integer> addvios = od.validForSample(data, errorRateThreshold, false, tupleNum);
                            if (addvios.isEmpty()) {
                                attributePairsToRemove.add(attributePair);
                                result.add(od);
                                ocdcount.getAndIncrement();
                            } else {
                                viorows.addAll(addvios);
                            }
                            validTime += timer.getTimeUsed() - temp;
                        }
                    }

                }
                for (AttributePair attributePair : attributePairsToRemove) {
                    csGet(context).remove(attributePair);
                }
            });

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

            if (singleAttributes.size() <= 1)
                continue;

            for (int i = 0; i < singleAttributes.size(); i++) {
                for (int j = i + 1; j < singleAttributes.size(); j++) {
                    boolean createContext = true;

                    AttributeSet candidate = prefix.addAttribute(singleAttributes.get(i))
                            .addAttribute(singleAttributes.get(j));

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
        PartialDataFrame sampleData = sampler.sample(data, config);
        System.out.println("|r'|:" + sampleData.getRowsCount());
        long sampleTime = spTime.getTimeUsed();

        HyODM f = new HyODM(30000000, -1f, Integer.parseInt(args[3]));

        f.initialize(data, sampleData, Integer.parseInt(args[2]));

        Set<CanonicalOD> result = f.discover(sampleData);

        long startM = r.freeMemory();
        long endM = r.totalMemory();
        System.out.println("MemoryCost: " + String.valueOf((endM - startM) / 1024 / 1024) + "MB");
        System.out.println(result);

        System.exit(0);
    }

}
