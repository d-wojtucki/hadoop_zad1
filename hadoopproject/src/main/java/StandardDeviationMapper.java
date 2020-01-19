import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StandardDeviationMapper extends Mapper<Object, Text, Text, DoubleWritable>{

    private Text keyText = new Text();
    private DoubleWritable keyValue = new DoubleWritable(0);

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String inputLine = value.toString();

        String fifth = inputLine.substring(inputLine.indexOf("fifth") + 7, inputLine.indexOf("fourth")-2);
        String fourth = inputLine.substring(inputLine.indexOf("fourth") + 8, inputLine.indexOf("first")-2);
        String first = inputLine.substring(inputLine.indexOf("first") + 7, inputLine.indexOf("third")-2);
        String third = inputLine.substring(inputLine.indexOf("third") + 7, inputLine.indexOf("second")-2);
        String second = inputLine.substring(inputLine.indexOf("second") + 8, inputLine.indexOf("side")-3);
        String side = Character.toString(inputLine.charAt(inputLine.indexOf("side") + 7));

        String series = inputLine.substring(inputLine.indexOf("series") + 8, inputLine.indexOf("sample")-2);
        //String sampleNumber = inputLine.substring(inputLine.indexOf("sample") + 8, inputLine.length()-1);

        String key_first = side + "-" + series + "-" + "first";
        String key_second = side + "-" + series + "-" + "second";
        String key_third = side + "-" + series + "-" + "third";
        String key_fourth = side + "-" + series + "-" + "fourth";
        String key_fifth = side + "-" + series + "-" + "fifth";

        keyText.set(key_first);
        keyValue.set(Double.parseDouble(first));
        context.write(keyText, keyValue);

        keyText.set(key_second);
        keyValue.set(Double.parseDouble(second));
        context.write(keyText, keyValue);

        keyText.set(key_third);
        keyValue.set(Double.parseDouble(third));
        context.write(keyText, keyValue);

        keyText.set(key_fourth);
        keyValue.set(Double.parseDouble(fourth));
        context.write(keyText, keyValue);

        keyText.set(key_fifth);
        keyValue.set(Double.parseDouble(fifth));
        context.write(keyText, keyValue);
    }
}
