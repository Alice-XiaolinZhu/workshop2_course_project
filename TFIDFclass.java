import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;

public class TFIDFclass {
	public static class Mapper3 extends Mapper<Object, Text, Text,Text>{
	private final static Text one = new Text("1");
	private Text key4 = new Text();
	private Text value4 = new Text();
	
	public void map(Object key, Text value,Context context) throws IOException,InterruptedException{
		
			key4.set(key.toString());
		    String val = value.toString();
		    String[] va = val.split(":");
		    key4.set(va[0]);
			value4.set(va[1]+":"+va[2]+":"+va[3]+":"+one);
			context.write(key4, value4);
		
	}
	} 
    public static class Reducer3 extends Reducer<Text,Text,Text,Text>{
        private Text finalkey =new Text();
        private Text finalvalue = new Text();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master-1730026126:9000");
			FileSystem fs=FileSystem.get(conf);
			Path filenamePath = new Path("hdfs://master-1730026126:9000/input/test");
			FileStatus stats[]=fs.listStatus(filenamePath);
			double number = stats.length;//get the number of documents
            double TF =0;
            double IDF=0;
            double dn=0;
            int i =0; 
            List<Double> stword = new ArrayList();//store the number of  word appearsed
            List<Double> sttotal = new ArrayList();//store the number of the total words number
            List<String> sttext = new ArrayList();//store the textname
            String[] val = null;
			for (Text value:values) {
				val=value.toString().split(":");
				stword.add(Double.parseDouble(val[1]));
				sttotal.add(Double.parseDouble(val[2]));
				sttext.add(val[0]);
				TF=Double.parseDouble(val[1])/Double.parseDouble(val[2]);
			    i+=1;
			    dn+=Double.parseDouble(val[3]);
			}
			for(int j=0;j<i;j++) {
				TF=stword.get(j)/sttotal.get(j);
				IDF=Math.log(number/(dn+1));
				double result= TF*IDF;
				finalvalue.set(Double.toString(result));
				String textname = sttext.get(j);
				finalkey.set(key.toString()+" :::::"+textname);
				context.write(finalkey, finalvalue);
			}
		    finalkey.set(Double.toString(TF));
				}
	}  
    
	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		PropertyConfigurator.configure("config/log4j.properties");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(TFIDFclass.class);
		job.setMapperClass(Mapper3.class);
		job.setReducerClass(Reducer3.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path("hdfs://master-1730026126:9000/output3"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master-1730026126:9000/output4"));
		System.exit(job.waitForCompletion(true)?0:1);
		long end = System.currentTimeMillis();
		System.out.print("time cost: "+(end-start)+"ms");
	}
}