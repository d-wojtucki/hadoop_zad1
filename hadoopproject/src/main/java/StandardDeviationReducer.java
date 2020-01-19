import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StandardDeviationReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    public List<Double> list = new ArrayList<Double>();
    public Text outputText = new Text();
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double sum = 0;
        double count = 0;
        list.clear();

        for (DoubleWritable doubleWritable : values) {
            sum = sum + doubleWritable.get();
            count = count + 1;
            list.add(doubleWritable.get());
        }
        double mean = sum / count;
        double sumOfSquares = 0;
        double sd = 0;
        for (double doubleWritable : list) {
            sumOfSquares += (doubleWritable - mean) * (doubleWritable - mean);
        }


        sd = (double) Math.sqrt(sumOfSquares / (count));
        outputText.set(String.valueOf(sd));
        context.write(key, outputText);
    }
}
