package fastod;

import dependencyDiscover.Data.DataFrame;
import dependencyDiscover.Predicate.Operator;
import dependencyDiscover.Predicate.SingleAttributePredicate;

import java.util.Objects;

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
        int contextValueDiff=context.getValue()-o.context.getValue();
        //如果attr不一样，也返回
        if(contextValueDiff!=0)
            return contextValueDiff;

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
    public boolean isValid(DataFrame data, double errorRateThreshold){
        //先得到分组，这个函数有个好处，即可以利用之前的结果去生成新的结果
        StrippedPartition sp= StrippedPartition.getStrippedPartition(context,data);
//        System.out.println(this);
//        System.out.println("isValid left: "+left);
//        System.out.println(sp);
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
}
