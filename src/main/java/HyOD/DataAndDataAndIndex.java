package HyOD;

/**
 * 用于记录
 */
public class DataAndDataAndIndex implements Comparable<DataAndDataAndIndex> {
    public int dataLeft;
    public int dataRight;
    public int index;

    @Override
    public int compareTo(DataAndDataAndIndex o) {
        return dataLeft-o.dataLeft;
    }

    @Override
    public String toString() {
        return "DataAndIndex{" +
                "dataLeft=" + dataLeft +
                "dataRight=" + dataRight +
                ", index=" + index +
                '}';
    }

    public DataAndDataAndIndex(int dataL,int dataR, int index) {
        this.dataLeft = dataL;
        this.dataRight = dataR;
        this.index = index;
    }
}
