package dependencyDiscover.MinimalityChecker;

import dependencyDiscover.Predicate.SingleAttributePredicate;
import dependencyDiscover.Predicate.SingleAttributePredicateList;

public class AlwaysOpenMinimalityChecker extends LODMinimalityChecker {
    @Override
    public int size() {
        return 0;
    }

    @Override
    public void insert(SingleAttributePredicateList left, SingleAttributePredicate right) {

    }

    @Override
    public boolean isMinimal(SingleAttributePredicateList listToAdd, SingleAttributePredicate predicateToAdd) {
        return true;
    }
}
