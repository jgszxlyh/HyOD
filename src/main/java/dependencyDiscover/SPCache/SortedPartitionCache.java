package dependencyDiscover.SPCache;

import dependencyDiscover.Predicate.SingleAttributePredicateList;
import dependencyDiscover.SortedPartition.SortedPartition;

public interface SortedPartitionCache {
    public SortedPartition get(SingleAttributePredicateList list);
}
