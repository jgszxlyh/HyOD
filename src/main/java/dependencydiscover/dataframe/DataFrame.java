package dependencydiscover.dataframe;

import util.Util;

import java.util.*;

public class DataFrame {
    protected List<List<Integer>> data = new ArrayList<>();
    private List<String> columnNames = new ArrayList<>();

    public int getColumnCount() {
        return columnNames.size();
    }

    public int getTupleCount() {
        return data.size();
    }

    public int get(int row, int column) {
        return data.get(row).get(column);
    }

    public List<Integer> getTuple(int tuple) {
        return data.get(tuple);
    }

    public void deleteRow(int row) {
        data.remove(row);
    }

    public void addTuple(List<Integer> tuple) {
        data.add(tuple);
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public int getRowCount() {
        return data.size();
    }

    public List<List<Integer>> getData() {
        return this.data;
    }

    public List<Integer> getRow(int row) {
        return data.get(row);
    }

    public static DataFrame fromCsv(String filePath) {
        try {
            String[] lines = Util.fromFile(filePath).split("\n");

            DataFrame result = new DataFrame();

            String line = lines[0];
            String[] parts = line.split(",");
            result.columnNames = Arrays.asList(parts);
            System.out.println("this file has " + lines.length + "lines");

            for (int i = 1; i < lines.length; i++) {
                line = lines[i];
                List<Integer> list = new ArrayList<>();
                parts = line.split(",");

                for (String part : parts) {
                    list.add(Integer.parseInt(part));
                }
                if (list.size() != result.getColumnCount()) {
                    throw new RuntimeException(String.format("column count not fixed:expected: %d, actual:%d",
                            result.getColumnCount(), list.size()));
                }

                result.data.add(list);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public String toAString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnNames.size(); i++) {
            if (i != 0)
                sb.append(',');
            sb.append(columnNames.get(i));
        }
        for (List<Integer> line : data) {
            sb.append('\n');
            for (int i = 0; i < line.size(); i++) {
                if (i != 0)
                    sb.append(',');
                sb.append(line.get(i));
            }
        }
        return sb.toString();
    }

    public void toCsv(String filePath) {
        Util.toFile(toAString(), filePath);
    }

    public DataFrame chooseColumns(Set<Integer> columnIndexes) {
        DataFrame result = new DataFrame();

        boolean[] choose = new boolean[getColumnCount()];
        int originalColumnCount = getColumnCount();
        for (Integer columnIndex : columnIndexes) {
            if (columnIndex >= 1 && columnIndex <= originalColumnCount)
                choose[columnIndex - 1] = true;
        }

        List<String> newColumnName = new ArrayList<>();
        for (int i = 0; i < originalColumnCount; i++) {
            if (choose[i]) {
                newColumnName.add(columnNames.get(i));
            }
        }
        result.columnNames = newColumnName;

        List<List<Integer>> newData = new ArrayList<>();
        for (List<Integer> originalRow : data) {
            List<Integer> newRow = new ArrayList<>();
            for (int i = 0; i < originalColumnCount; i++) {
                if (choose[i]) {
                    newRow.add(originalRow.get(i));
                }
            }
            newData.add(newRow);
        }
        result.data = newData;
        return result;
    }

    public DataFrame randomSelectColumns(int columnCount) {
        if (columnCount > getColumnCount() || columnCount < 0) {
            return this;
        }
        List<Integer> columns = new ArrayList<>();
        for (int column = 1; column <= getColumnCount(); column++) {
            columns.add(column);
        }
        Collections.shuffle(columns);
        Set<Integer> columnToChoose = new HashSet<>();
        for (int i = 0; i < columnCount; i++) {
            columnToChoose.add(columns.get(i));
        }
        chooseColumns(columnToChoose);
        return this;
    }

    public interface ThreeIntegerConsumer {
        int consume(int tuple, int column, int randomResult);
    }

    public boolean tupleEqual(int tuple1, int tuple2) {
        for (int attribute = 0; attribute < getColumnCount(); attribute++) {
            if (get(tuple1, attribute) != get(tuple2, attribute))
                return false;
        }
        return true;
    }

    public boolean tupleEqual(int tuple1, int tuple2, Collection<Integer> attributes) {
        for (Integer attribute : attributes) {
            if (get(tuple1, attribute) != get(tuple2, attribute))
                return false;
        }
        return true;
    }

    public DataFrame getSubDataFrameFromFirstTuplesAndColumns(int countTuple, int countColumn) {
        Set<Integer> preservedTuples = new HashSet<>();
        for (int i = 0; i < countTuple; i++) {
            preservedTuples.add(i);
        }
        Set<Integer> preservedColumns = new HashSet<>();
        for (int i = 0; i < countColumn; i++) {
            preservedColumns.add(i);
        }
        return getSubDataFrame(preservedTuples, preservedColumns);
    }

    public DataFrame getRandomSubDataFrame(int countTuple, int countColumn) {
        if (countTuple > getTupleCount()) {
            countTuple = getTupleCount();
        }
        if (countColumn > getColumnCount()) {
            countColumn = getColumnCount();
        }
        if (countTuple == getTupleCount() && countColumn == getColumnCount()) {
            return this;
        }
        Set<Integer> preservedTuples = new HashSet<>(Util.randomSample(getTupleCount(), countTuple));
        Set<Integer> preservedColumns = new HashSet<>(Util.randomSample(getColumnCount(), countColumn));
        return getSubDataFrame(preservedTuples, preservedColumns);
    }

    public DataFrame getSubDataFrame(Set<Integer> preservedTuples, Set<Integer> preservedColumns) {
        DataFrame result = new DataFrame();
        for (int column = 0; column < columnNames.size(); column++) {
            if (preservedColumns.contains(column)) {
                result.columnNames.add(columnNames.get(column));
            }
        }
        for (int tuple = 0; tuple < getTupleCount(); tuple++) {
            if (!preservedTuples.contains(tuple)) {
                continue;
            }
            List<Integer> newTuple = new ArrayList<>();
            for (int column = 0; column < getColumnCount(); column++) {
                if (preservedColumns.contains(column)) {
                    newTuple.add(data.get(tuple).get(column));
                }
            }
            result.data.add(newTuple);
        }
        return result;
    }

}
