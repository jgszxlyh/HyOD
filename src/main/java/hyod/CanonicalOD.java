package hyod;

import java.util.*;

import dependencydiscover.dataframe.DataFrame;
import dependencydiscover.predicate.Operator;
import dependencydiscover.predicate.SingleAttributePredicate;

public class CanonicalOD implements Comparable<CanonicalOD> {

    public AttributeSet context;
    public int right;
    public SingleAttributePredicate left;
    public static int splitCheckCount = 0;
    public static int swapCheckCount = 0;

    @Override
    public int compareTo(CanonicalOD o) {
        int attributeCountDifference = context.getAttributeCount() - o.context.getAttributeCount();

        if (attributeCountDifference != 0)
            return attributeCountDifference;
        long contextValueDiff = context.getValue() - o.context.getValue();

        if (contextValueDiff != 0)
            return (int) contextValueDiff;

        int rightDiff = right - o.right;
        if (rightDiff != 0)
            return rightDiff;
        if (left != null) {
            int leftDiff = left.attribute - o.left.attribute;
            if (leftDiff != 0)
                return leftDiff;
            if (left.operator == o.left.operator)
                return 0;
            if (left.operator == Operator.LESSEQUAL)
                return -1;
        }
        return 0;

    }

    public CanonicalOD(AttributeSet context, SingleAttributePredicate left, int right) {
        this.context = context;
        this.right = right;
        this.left = left;
    }

    public CanonicalOD(AttributeSet context, int right) {
        this.context = context;
        this.right = right;
        this.left = null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(context).append(" : ");
        if (left == null) {
            sb.append("[] -> ");
        } else {
            sb.append(left).append(" ~ ");
        }
        sb.append(right + 1).append("<=");
        return sb.toString();
    }

    public boolean isValid(DataFrame data, double errorRateThreshold, boolean isSample) {

        StrippedPartition sp = StrippedPartition.getStrippedPartition(context, data, isSample);

        if (errorRateThreshold == -1f) {

            if (left == null) {
                splitCheckCount++;
                return !sp.split(right);
            }

            else {
                swapCheckCount++;
                return !sp.swap(left, right);
            }
        } else {
            long vioCount;
            if (left == null) {
                System.out.println("split");
                vioCount = sp.splitRemoveCount(right);
            } else {
                System.out.println("swap");

                vioCount = sp.swapRemoveCountEDBT(left, right);
                System.out.println("vioCount: " + vioCount);
            }
            System.out.println();
            double errorRate = (double) vioCount / data.getTupleCount();
            return errorRate < errorRateThreshold;
        }
    }

    public static void clearForResult(boolean isSample) {
        StrippedPartition sp = new StrippedPartition();
        sp.clearCache(isSample);
    }

    public Set<Integer> validForViorows(DataFrame data, double errorRateThreshold, boolean isSample) {

        StrippedPartition sp = StrippedPartition.getStrippedPartition(context, data, isSample);

        if (sp.isUCC()) {
            return new HashSet<>();
        }

        if (errorRateThreshold == -1f) {

            if (left == null) {
                splitCheckCount++;
                return sp.splitForViorows(right);
            }

            else {
                swapCheckCount++;
                return sp.swapForViorows(left, right);
            }
        }
        return new HashSet<>();
    }

    public Set<Integer> validForSample(DataFrame data, double errorRateThreshold, boolean isSample, int vioNum) {

        StrippedPartition sp = StrippedPartition.getStrippedPartition(context, data, isSample);

        if (sp.isUCC()) {
            return new HashSet<>();
        }

        if (errorRateThreshold == -1f) {

            if (left == null) {
                splitCheckCount++;
                return sp.splitForSample(right, vioNum);
            }

            else {
                swapCheckCount++;
                return sp.swapForSample(left, right, vioNum);
            }
        }
        return new HashSet<>();
    }

    public Set<Integer> validForRandom(DataFrame data, double errorRateThreshold, boolean isSample) {

        StrippedPartition sp = StrippedPartition.getStrippedPartition(context, data, isSample);

        if (sp.isUCC()) {
            return new HashSet<>();
        }

        if (errorRateThreshold == -1f) {

            if (left == null) {
                splitCheckCount++;
                return sp.splitForRandom(right);
            }

            else {
                swapCheckCount++;
                return sp.swapForRandom(left, right);
            }
        }
        return new HashSet<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof CanonicalOD))
            return false;
        CanonicalOD that = (CanonicalOD) o;
        return right == that.right &&
                left == that.left &&
                context.equals(that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, right, left);
    }

    public String toHashString() {
        StringBuilder sb = new StringBuilder();
        sb.append(context.toOrderedList());
        if (left != null)
            sb.append(left.toHashString());

        sb.append(right);
        return sb.toString();
    }

    // public static void main(String[] args) {
    //     Set<Integer> as = new HashSet<>();
    //     SingleAttributePredicate left1 = new SingleAttributePredicate(0, Operator.GREATEREQUAL);
    //     SingleAttributePredicate left2 = new SingleAttributePredicate(0, Operator.LESSEQUAL);
    //     CanonicalOD od1 = new CanonicalOD(new AttributeSet(as), left1, 1);
    //     CanonicalOD od2 = new CanonicalOD(new AttributeSet(as), left2, 1);

    //     System.out.println(od1);
    //     System.out.println(od1.isValid(DataFrame.fromCsv("Data/test1.csv"), -1f, false));
    //     System.out.println(od2);
    //     System.out.println(od2.isValid(DataFrame.fromCsv("Data/test1.csv"), -1f, false));
    // }
}
