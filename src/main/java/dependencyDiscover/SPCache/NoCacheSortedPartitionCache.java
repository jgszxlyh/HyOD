package dependencyDiscover.SPCache;

import dependencyDiscover.Data.DataFrame;
import dependencyDiscover.Predicate.SingleAttributePredicate;
import dependencyDiscover.Predicate.SingleAttributePredicateList;
import dependencyDiscover.SortedPartition.SortedPartition;

public class NoCacheSortedPartitionCache implements SortedPartitionCache{

    private DataFrame data;

    public NoCacheSortedPartitionCache (DataFrame data){

        this.data = data;
    }
    @Override
    public SortedPartition get(SingleAttributePredicateList list) {
        SortedPartition sp=new SortedPartition(data);
        for (SingleAttributePredicate predicate : list) {
            sp.intersect(data,predicate);
        }
        return sp;
    }
}
