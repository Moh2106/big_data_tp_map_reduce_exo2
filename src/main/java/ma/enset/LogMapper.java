package ma.enset;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        String[] lines = value.toString().split(" ");
        String http = lines[0];
        int ip = Integer.parseInt(lines[4]);
        int nbreReussi = Integer.parseInt(lines[5]);

        if (ip == 200){
            context.write(new Text(http + "ip"), new IntWritable( nbreReussi));
        }
    }
}
