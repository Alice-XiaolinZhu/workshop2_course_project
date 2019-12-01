import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;

public class task3 {
	public static class Mapper1 extends Mapper<Object, Text, Text,Text>{
		
	private final static Text one = new Text();
	private Text word = new Text();
	private String pattern = "[^a-zA-Z]";
	private FileSplit split;
	int n =0;
	public void map(Object key, Text value,Context context) throws IOException,InterruptedException{

		split =(FileSplit)context.getInputSplit();
		String itr1 = value.toString();
		itr1 = itr1.replaceAll(pattern," ");//to clean data
		StringTokenizer itr = new StringTokenizer(itr1);
		String line = new String();
		String s="";

		n = n+1; 
		while(itr.hasMoreTokens()) {
			s= String.valueOf(n);
			String splitIndex =split.getPath().getName().toString();
			line = line + " , "+s;//add the line number
			one.set(s);
			word.set(itr.nextToken()+"&&&&&"+splitIndex);//used to identify filename
			context.write(word,one);
		}
	}}
	public static class Reducer1 extends Reducer<Text,Text,Text,Text>{
	    private Text t1 = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context)throws IOException,InterruptedException{
	    int count = 0; //store the times of word appeared
	    String s1 = "";
		for (Text value:values) {
			count+=1;
			s1=value.toString()+","+s1;
		}
		String cou = String.valueOf(count);
		cou = "....:"+cou; //use to split
		t1.set(cou+",,,,"+s1);
		context.write(key,t1);
		}
	}
	
	public static class Mapper2 extends Mapper<Object,Text,Text,Text>{
		private Text t2 = new Text(); 
		private Text t3 = new Text();
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
		String douc = new String();
		String wor = new String();
		String[] st = value.toString().split("\\&&&&&"); //split the value and put it into output key and output value
		String[] st2 = st[1].split("\\....:");
		String[] st3 = st2[1].split(",,,,");
        t3.set(st2[0]+"$$$$$["+st3[1]+"@@@@@]");
        t2.set(st[0]+":::"+st3[0]);
		context.write(t3,t2);
	}
	}
	public static class Reducer2 extends Reducer<Text,Text,Text,Text>{
	    private Text keyreduce2 = new Text();
	    private Text valuereduce2 = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context)throws IOException,InterruptedException{
		int count =0;//used to store the  number of document
		int i=0;
		String[] st = null;
		List<String> stword = new ArrayList();// use two arraylist to store  information 
		List<String> stnum = new ArrayList();
		for(Text value:values) {
			st =value.toString().split("\\:::");
			 count += Integer.parseInt(st[1]);
			 stword.add(st[0]);
			 stnum.add(st[1]);
			 i +=1;
		}
		stword.toArray();
		for(int j=0;j<i;j++) { 
			String cou = String.valueOf(count); 
			valuereduce2.set(stnum.get(j)+":"+cou);
			keyreduce2.set(stword.get(j)+":"+key+":");
			context.write(keyreduce2, valuereduce2);
		}
		}
	}  
	
	
	public static class Mapper3 extends Mapper<Object,Text,Text,Text>{
		private Text key3 = new Text();
		private Text value3 = new Text();
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
		String k3 = new String();
		String v3 = new String();
		k3=key.toString();
		v3=value.toString();
		key3.set(k3);
		value3.set(v3);
		context.write(key3,value3);
	}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("config/log4j.properties");
		Configuration conf1 = new Configuration();
		Configuration conf2 = new Configuration();
		
		Job job1 = Job.getInstance(conf1);
		job1.setJarByClass(task3.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
     	job1.setMapOutputKeyClass(Text.class);
     	job1.setMapOutputValueClass(Text.class);
     	job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1,new Path("hdfs://master-1730026126:9000/input/test"));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://master-1730026126:9000/output2"));



		
		Job job2 = Job.getInstance(conf2);

		
		job2.setJarByClass(task3.class);
    	job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
     	job2.setMapOutputKeyClass(Text.class);
     	job2.setMapOutputValueClass(Text.class);
     	job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2,new Path("hdfs://master-1730026126:9000/output2"));
		FileOutputFormat.setOutputPath(job2, new Path("hdfs://master-1730026126:9000/output3"));

		
		
		JobControl jobCtrl = new JobControl("myCtril");
		ControlledJob ctrlJob1 = new ControlledJob(job1.getConfiguration());
		ctrlJob1.setJob(job1);
		ControlledJob ctrlJob2 = new ControlledJob(job2.getConfiguration());
		ctrlJob2.setJob(job2);
		ctrlJob2.addDependingJob(ctrlJob1);

		jobCtrl.addJob(ctrlJob1);
		jobCtrl.addJob(ctrlJob2);
		
		Thread thread =new Thread(jobCtrl);
		thread.start();
		while(true) {
			if(jobCtrl.allFinished()) {
				System.out.println(jobCtrl.getSuccessfulJobList());
				jobCtrl.stop();
				break;
			}
		}
		System.exit(job2.waitForCompletion(true)?0:1);

	}
}