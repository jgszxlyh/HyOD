package HyOD;

import dependencyDiscover.Data.DataFrame;
import dependencyDiscover.Predicate.Operator;
import dependencyDiscover.Predicate.SingleAttributePredicate;

import java.util.*;

public class CanonicalOD implements Comparable<CanonicalOD>{

    public AttributeSet context;
    public int right;
    public SingleAttributePredicate left;
    public static int splitCheckCount=0;
    public static int swapCheckCount=0;

    @Override
    public int compareTo(CanonicalOD o) {
        int attributeCountDifference=context.getAttributeCount()-o.context.getAttributeCount();
        //如果attr的个数都不等，则直接返回
        if(attributeCountDifference!=0)
            return attributeCountDifference;
        long contextValueDiff=context.getValue()-o.context.getValue();
        //如果attr不一样，也返回
        if(contextValueDiff!=0)
            return (int)contextValueDiff;

        int rightDiff=right-o.right;
        if (rightDiff!=0)
            return rightDiff;
        if (left!=null) {
            int leftDiff = left.attribute - o.left.attribute;
            if (leftDiff != 0)
                return leftDiff;
            if (left.operator == o.left.operator)
                return 0;
            if (left.operator == Operator.lessEqual)
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
        this.left=null;
    }

    /**
     * 用的都是index，left因为是SingleAttributePredicate，在该类的toString方法中已经做过+1操作了
     * @return
     */
    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(context).append(" : ");
        if(left==null){
            sb.append("[] -> ");
        }else {
            sb.append(left).append(" ~ ");
        }
        sb.append(right+1).append("<=");
        return sb.toString();
    }

    /**
     * 验证OD是否成立
     * @param data 数据集
     * @param errorRateThreshold 错误率，用来实现近似OD
     * @return 是否成立
     */


    public boolean isValid(DataFrame data, double errorRateThreshold,boolean isSample){
        //先得到分组，这个函数有个好处，即可以利用之前的结果去生成新的结果
        StrippedPartition sp= StrippedPartition.getStrippedPartition(context,data,isSample);
        //-1f指关闭错误率阈值，要完全保证无swap和split
        if (errorRateThreshold==-1f){
            //左边为空，说明是第一种od，即使用FD检验
            if(left==null){
                splitCheckCount++;
                return !sp.split(right);
            }
            //左侧要是有元素，一定是OCD，判断swap
            else {
                swapCheckCount++;
                return !sp.swap(left, right);
            }
        }else {
            long vioCount;
            if (left == null) {
                System.out.println("split");
                vioCount = sp.splitRemoveCount(right);
            } else {
                System.out.println("swap");
//              vioCount = sp.swapRemoveCount(left,right);
                vioCount = sp.swapRemoveCountEDBT(left,right);
                System.out.println("vioCount: " + vioCount);
            }
            System.out.println();
            double errorRate = (double) vioCount /data.getTupleCount();
            return errorRate<errorRateThreshold;
        }
    }




    public static void clearForResult(boolean isSample){
        StrippedPartition sp = new StrippedPartition();
        sp.clearCache(isSample);
    }

    public Set<Integer> validForViorows(DataFrame data, double errorRateThreshold,boolean isSample){
        //先得到分组，这个函数有个好处，即可以利用之前的结果去生成新的结果
        StrippedPartition sp= StrippedPartition.getStrippedPartition(context,data,isSample);

        if(sp.isUCC())
        {
            return new HashSet<>();
        }
//        System.out.println(this);
//        System.out.println("isValid left: "+left);
//        System.out.println(sp);
        //-1f指关闭错误率阈值，要完全保证无swap和split
        if (errorRateThreshold==-1f){
            //左边为空，说明是第一种od，即使用FD检验
            if(left==null){
                splitCheckCount++;
                return sp.splitForViorows(right);
            }
            //左侧要是有元素，一定是OCD，判断swap
            else {
                swapCheckCount++;
                return sp.swapForViorows(left,right);
            }
        }
        return new HashSet<>();
    }
    public Set<Integer> validForSample(DataFrame data, double errorRateThreshold,boolean isSample,int vioNum){
        //先得到分组，这个函数有个好处，即可以利用之前的结果去生成新的结果
        StrippedPartition sp= StrippedPartition.getStrippedPartition(context,data,isSample);

        if(sp.isUCC())
        {
            return new HashSet<>();
        }
//        System.out.println(this);
//        System.out.println("isValid left: "+left);
//        System.out.println(sp);
        //-1f指关闭错误率阈值，要完全保证无swap和split
        if (errorRateThreshold==-1f){
            //左边为空，说明是第一种od，即使用FD检验
            if(left==null){
                splitCheckCount++;
                return sp.splitForSample(right,vioNum);
            }
            //左侧要是有元素，一定是OCD，判断swap
            else {
                swapCheckCount++;
                return sp.swapForSample(left,right,vioNum);
            }
        }
        return new HashSet<>();
    }

    public Set<Integer> validForRandom(DataFrame data, double errorRateThreshold,boolean isSample){
        //先得到分组，这个函数有个好处，即可以利用之前的结果去生成新的结果
        StrippedPartition sp= StrippedPartition.getStrippedPartition(context,data,isSample);

        if(sp.isUCC())
        {
            return new HashSet<>();
        }
//        System.out.println(this);
//        System.out.println("isValid left: "+left);
//        System.out.println(sp);
        //-1f指关闭错误率阈值，要完全保证无swap和split
        if (errorRateThreshold==-1f){
            //左边为空，说明是第一种od，即使用FD检验
            if(left==null){
                splitCheckCount++;
                return sp.splitForRandom(right);
            }
            //左侧要是有元素，一定是OCD，判断swap
            else {
                swapCheckCount++;
                return sp.swapForRandom(left,right);
            }
        }
        return new HashSet<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CanonicalOD)) return false;
        CanonicalOD that = (CanonicalOD) o;
        return right == that.right &&
                left == that.left &&
                context.equals(that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, right, left);
    }

    /**
     * 将类对象转化成String用于哈希编码
     * @return
     */
    public String toHashString(){
        StringBuilder sb = new StringBuilder();
        sb.append(context.toOrderedList());
        if(left!=null)
            sb.append(left.toHashString());
        //这里是index，不加
        sb.append(right);
        return sb.toString();
    }

    public static void main(String[] args) {
        Set<Integer> as = new HashSet<>();
        SingleAttributePredicate left1 = new SingleAttributePredicate(0,Operator.greaterEqual);
        SingleAttributePredicate left2 = new SingleAttributePredicate(0,Operator.lessEqual);
        CanonicalOD od1 = new CanonicalOD(new AttributeSet(as),left1,1);
        CanonicalOD od2 = new CanonicalOD(new AttributeSet(as),left2,1);
//        as.add(0);
////        CanonicalOD od = new CanonicalOD(new AttributeSet(as),0);
        System.out.println(od1);
        System.out.println(od1.isValid(DataFrame.fromCsv("Data/test1.csv"),-1f,false));
        System.out.println(od2);
        System.out.println(od2.isValid(DataFrame.fromCsv("Data/test1.csv"),-1f,false));
    }
}
