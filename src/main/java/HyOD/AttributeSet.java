
package hyod;

import java.util.Collection;
import java.util.Iterator;

public class AttributeSet implements Iterable<Integer> {
    private final long value;

    public long getValue() {
        return value;
    }

    public AttributeSet() {
        this(0);
    }

    public AttributeSet(long value) {
        this.value = value;
    }

    public AttributeSet(Collection<Integer> attributes) {
        long sum = 0;
        for (int attribute : attributes) {
            sum += (1L << attribute);
        }
        value = sum;
    }

    @Override
    public Iterator<Integer> iterator() {
        return new AttributeSetIterator();
    }

    public boolean containAttribute(int attribute) {
        return (value & 1L << attribute) != 0;
    }

    public AttributeSet addAttribute(int attribute) {
        if (containAttribute(attribute)) {
            return this;
        } else {
            return new AttributeSet(value | 1L << attribute);
        }
    }

    public AttributeSet deleteAttribute(int attribute) {
        if (containAttribute(attribute)) {
            return new AttributeSet(value ^ 1L << attribute);
        } else {
            return this;
        }
    }

    public class AttributeSetIterator implements Iterator<Integer> {
        private int pointer;

        public AttributeSetIterator() {
            pointer = -1;
            findNext();
        }

        private void findNext() {
            pointer++;
            while (pointer < 64 && !containAttribute(pointer)) {
                pointer++;
            }
        }

        @Override
        public boolean hasNext() {
            return pointer < 64;
        }

        @Override
        public Integer next() {
            int result = pointer;
            findNext();
            return result;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (long attribute : this) {
            if (first)
                first = false;
            else
                sb.append(',');
            sb.append(attribute + 1);
        }
        sb.append('}');
        return sb.toString();
    }

    public AttributeSet union(AttributeSet as2) {
        return new AttributeSet(value | as2.value);
    }

    public AttributeSet intersect(AttributeSet as2) {
        return new AttributeSet(value & as2.value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AttributeSet that = (AttributeSet) o;
        return value == that.value;
    }

    public int getAttributeCount() {
        return Long.bitCount(value);
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    public int getFirstAttribute() {
        for (int i = 0; i < 64; i++) {
            if (containAttribute(i))
                return i;
        }
        throw new RuntimeException("找不到属性");
    }

    public int getLastAttribute() {
        for (int i = 63; i >= 0; i--) {
            if (containAttribute(i))
                return i;
        }
        throw new RuntimeException("找不到属性");
    }

    public AttributeSet difference(AttributeSet as2) {
        return new AttributeSet(value & (~0 ^ as2.value));
    }

    public boolean isEmpty() {
        return value == 0;
    }

    public String toOrderedList() {
        StringBuilder sb = new StringBuilder();
        for (long attribute : this) {
            sb.append(attribute + 1);
        }
        return sb.toString();
    }

    public int getSize() {
        int count = 0, ptr = 1;
        for (int i = 0; i < 64; i++) {
            if ((value & ptr) != 0)
                count++;
            ptr = ptr << 1;
        }
        return count;
    }

    public static void main(String[] args) {
        AttributeSet test = new AttributeSet(2);
        AttributeSet test1 = new AttributeSet(2);
        test.deleteAttribute(2);
        test1.deleteAttribute(2);
        System.out.println(test.equals(test1));
    }
}
