// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context)
            throws IOException, InterruptedException {

        String mergeValue = "";
        for (Text value : values) {
            String curValue = value.toString();
            mergeValue = mergeValue + " " + curValue;
//            maxValue = Math.max(maxValue, value.get());
        }
        context.write(key, new Text(mergeValue));
    }
}
// ^^ MaxTemperatureReducer