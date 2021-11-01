package um.si;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();

        String itemName = row.split(",")[2];
        int quantity = Integer.valueOf(row.split(",")[3]);

        context.write(new Text(itemName), new IntWritable(quantity));
    }
}
