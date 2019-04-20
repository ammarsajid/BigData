import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task3b {

  public static class Task3bMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{

    private Text word = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(),",");
      while (itr.hasMoreTokens()) {
    	
    	// Skipping first 3 columns
    	itr.nextToken();
    	itr.nextToken();
    	// To Skip the first row that is headings of CSV data
    	try{ Integer.parseInt(itr.nextToken());}
    	catch(Exception e) { return; }
    	
    	word.set("malekey");
    	DoubleWritable male_litracy_rate = new DoubleWritable(Double.parseDouble( itr.nextToken()));
        context.write(word, male_litracy_rate);
        
        word.set("femalekey");
        DoubleWritable female_litracy_rate = new DoubleWritable(Double.parseDouble( itr.nextToken()));
        context.write(word, female_litracy_rate);
      }
    }
  }

  public static class Task3bReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
      double sum = 0;
      int counter = 0;
      for (DoubleWritable val : values) {
        sum += val.get();
        counter++;
      }
     
      result.set(sum/counter);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "task3b");
    job.setJarByClass(Task3b.class);
    job.setMapperClass(Task3bMapper.class);
    job.setCombinerClass(Task3bReducer.class);
    job.setReducerClass(Task3bReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}