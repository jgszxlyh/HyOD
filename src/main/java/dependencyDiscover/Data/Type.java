package dependencyDiscover.Data;

import java.util.Comparator;

public interface Type{
    boolean fitFormat(String s);
    Object parse(String s);
    Comparator getComparator();
}
