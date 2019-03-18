import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task3a {

  public static class Task3aMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(),",");
      while (itr.hasMoreTokens()) {
    	itr.nextToken();	// Skipping first Column (Districts)
    	
    	// To Skip the first row that is headings of CSV data
    	String temp_str = itr.nextToken(); 
    	try{ Integer.parseInt(temp_str);}
    	catch(Exception e) { return; }
    	
        word.set("malekey");
    	IntWritable male_value = new IntWritable(Integer.parseInt( temp_str));
        context.write(word, male_value);
        
        temp_str = itr.nextToken();
        word.set("femalekey");
        IntWritable female_value = new IntWritable(Integer.parseInt( temp_str));
        context.write(word, female_value);
        
        // Skipping next two columns  (Literacy Rate Male & Literacy Rate Female)
        itr.nextToken();
        itr.nextToken();
                    
      }
    }
  }

  public static class Task3aReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "task3a");
    job.setJarByClass(Task3a.class);
    job.setMapperClass(Task3aMapper.class);
    job.setCombinerClass(Task3aReducer.class);
    job.setReducerClass(Task3aReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}