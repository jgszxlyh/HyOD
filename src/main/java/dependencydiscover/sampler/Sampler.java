package dependencydiscover.sampler;

import java.util.Random;
import java.util.Set;

import dependencydiscover.dataframe.DataFrame;
import dependencydiscover.dataframe.PartialDataFrame;

public abstract class Sampler {
    protected Random random;

    public Sampler() {
        random = new Random();
    }

    public void setRandomSeed(long randomSeed) {
        random.setSeed(randomSeed);
    }

    final public PartialDataFrame sample(DataFrame data) {
        int sampleRowCount = Math.max(5, Math.min(data.getRowCount() / 100, 100));
        return sample(data, new SampleConfig(sampleRowCount));
    }

    final public PartialDataFrame sample(DataFrame data, SampleConfig adviceConfig) {
        if (data == null)
            return null;
        int dataLineCount = data.getRowCount();

        int sampleLineCount = adviceConfig.isUsePercentage() ? (int) (dataLineCount * adviceConfig.samplePercentage)
                : adviceConfig.sampleLineCount;
        if (sampleLineCount > dataLineCount)
            sampleLineCount = dataLineCount;

        Set<Integer> sampleLines = chooseLines(data, sampleLineCount);
        PartialDataFrame result = new PartialDataFrame(data, sampleLines);
        result.setColumnNames(data.getColumnNames());
        return result;
    }

    protected abstract Set<Integer> chooseLines(DataFrame data, int adviceSampleSize);

}
