import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;

public class task2 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text,Text>{
	private Text txt = new Text();
	private Text word = new Text();
	private String pattern = "[^a-zA]";
	private FileSplit split;
	int line =0;  


	public void map(Object key, Text value,Context context) throws IOException,InterruptedException{
		

		String s="";
		String line = new String();
		
		split =(FileSplit)context.getInputSplit(); 
		String itr1 = value.toString();
		itr1 = itr1.replaceAll(pattern," ");//used to clean the data
		StringTokenizer itr = new StringTokenizer(itr1);
		
		String splitIndex =split.getPath().getName();//get filename
		line = line + " , "+s; 
		line = line+1; 
			while(itr.hasMoreTokens()) {
			s= String.valueOf(line);
			
			line = line + " , "+s;//add the line number
			txt.set(s);
			String ws = itr.nextToken()+","+splitIndex;//used to split 
			word.set(ws);
			context.write(word,txt);
		}
	}
	}
	
	public static class InvertedIndexCombiner extends Reducer<Text,Text,Text,Text>{
	    private Text t1 = new Text();
	    private Text t2 = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context)throws IOException,InterruptedException{
		String[] st=key.toString().split("\\,");//split by ,
		String w=st[1];
		String word=st[0];
		for (Text value:values) {
			w+=" "+value.toString()+" ";
		}
				t2.set(w);
		t1.set(word.toString());
		context.write(t1, t2);
		}
	}
	public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>{//to show the same word in one line, we need another combiner
		Text t3 = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context)throws IOException,InterruptedException{
		String word = new String();
		for (Text value:values) {
			word = word+"["+value.toString()+"]";
		}
		t3.set(word);
		context.write(key, t3);
		}
	}
	
	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		PropertyConfigurator.configure("config/log4j.properties");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(task2.class);
		job.setMapperClass(TokenizerMapper.class);
		
		
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setReducerClass(InvertedIndexReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("hdfs://master-17300261262:9000/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master-17300261262:9000/output1"));
		System.exit(job.waitForCompletion(true)?0:1);
		long end = System.currentTimeMillis();
		System.out.print("time cost: "+(end-start)+"ms");//to get the time
	}
}
