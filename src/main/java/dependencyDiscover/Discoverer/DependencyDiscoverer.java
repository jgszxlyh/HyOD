package dependencyDiscover.Discoverer;

import dependencyDiscover.Data.DataFrame;
import dependencyDiscover.Dependency.AbstractDependency;

import java.util.Collection;

public interface DependencyDiscoverer<E extends AbstractDependency> {
    public Collection<E> discover(DataFrame data);
}
