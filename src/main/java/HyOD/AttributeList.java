package hyod;

import java.util.*;
import java.util.function.Consumer;

public class AttributeList implements Iterable<Integer> {
    private final List<Integer> attributeList;

    public List<Integer> getValue() {
        return new ArrayList<>();
    }

    public Integer getListLength() {
        return this.attributeList.size();
    }

    public Integer getAttributeCount() {
        return this.attributeList.size();
    }

    public AttributeList() {
        this.attributeList = new ArrayList<>();
    }

    public AttributeList(Collection<Integer> attributes) {
        List<Integer> temp = new ArrayList<>();
        for (int attribute : attributes) {
            temp.add(attribute);
        }
        this.attributeList = new ArrayList<>(temp);
    }

    public AttributeList(AttributeList attributeList, Integer a, Integer b) {
        List<Integer> temp = new ArrayList<>();
        for (int attribute : attributeList) {
            temp.add(attribute);
        }
        temp.add(a);
        temp.add(b);
        this.attributeList = new ArrayList<>(temp);
    }

    public AttributeList(Integer attribute) {
        this.attributeList = new ArrayList<Integer>() {
            {
                add(attribute);
            }
        };
    }

    @Override
    public Iterator<Integer> iterator() {
        return new AttributeListIterator();
    }

    public class AttributeListIterator implements Iterator<Integer> {
        private int pointer;

        public AttributeListIterator() {
            pointer = 0;
        }

        @Override
        public boolean hasNext() {
            return pointer < AttributeList.this.attributeList.size();
        }

        @Override
        public Integer next() {

            int result = AttributeList.this.attributeList.get(pointer);
            pointer++;
            return result;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (int attribute : this.attributeList) {
            if (first)
                first = false;
            else
                sb.append(',');
            sb.append(attribute);
        }
        sb.append('}');
        return sb.toString();
    }

    public String getSubString(int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int attribute : this.attributeList)
            sb.append(attribute);
        return sb.toString().substring(start, end);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AttributeList integers = (AttributeList) o;
        return Objects.equals(attributeList, integers.attributeList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributeList);
    }

    public AttributeList getPrefix() {
        if (this.attributeList.size() <= 1)
            return new AttributeList();
        else {

            return new AttributeList(this.attributeList.subList(0, this.attributeList.size() - 1));
        }
    }

    public Integer getLast() {
        return attributeList.get(attributeList.size() - 1);
    }

    public AttributeList appendLast(Integer a, Integer b) {
        return new AttributeList(this, a, b);
    }

    @Override
    public void forEach(Consumer<? super Integer> action) {
        Iterable.super.forEach(action);
    }

    @Override
    public Spliterator<Integer> spliterator() {
        return Iterable.super.spliterator();
    }

    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i + 1);
        }
        AttributeList at = new AttributeList(list);
        System.out.println(at.getPrefix());
    }
}
