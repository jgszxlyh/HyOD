package HyOD;

import dependencyDiscover.Predicate.SingleAttributePredicate;

import java.security.InvalidParameterException;
import java.util.Objects;

public class AttributePair {
    public final SingleAttributePredicate left;
    public final int right;

    public AttributePair(SingleAttributePredicate left, int right) {
        if(left.attribute==right){
            throw new InvalidParameterException("two attributes cannot be the same");
        }
        this.left = left;
        this.right = right;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AttributePair)) return false;
        AttributePair that = (AttributePair) o;
        return right == that.right && left==that.left ||right == that.left.attribute && left.attribute==that.right;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(left.attribute)+Objects.hashCode(right);
    }

    @Override
    public String toString() {
        return String.format("{%s,%d}", left, right +1);
    }


}
