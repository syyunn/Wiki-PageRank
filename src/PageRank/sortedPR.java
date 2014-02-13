package PageRank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class sortedPR {

	public static class sortPRMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration() ;
			long numTitles = Long.parseLong(conf.get("NUM_TITLES")) ;   // Integer.parseInt(NUM_TITLES) ;
			float compareVal = (float) (5.0/numTitles) ;
			
			int titleTabIndex = value.find("\t") ;
			String title = Text.decode(value.getBytes(),0,titleTabIndex) ;
			
			int rankTabIndex = value.find("\t", titleTabIndex+1) ;
			
			String rankStr = null ;

			if (rankTabIndex != -1) 
				rankStr = Text.decode(value.getBytes(), titleTabIndex+1, rankTabIndex-(titleTabIndex+1)) ;
			else
				rankStr = Text.decode(value.getBytes(), titleTabIndex+1, value.getLength()-(titleTabIndex+1)) ;

			float currentRank = Float.parseFloat(rankStr) ;
			
			if (currentRank < compareVal) return ;
			
			FloatWritable keyRank = new FloatWritable(currentRank) ;
			Text valueTitle = new Text(title) ;
			
			
		
			
			context.write(keyRank, valueTitle);


				
		} // end map function

	} // end mapper class

	public static class sortPRReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {

		public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Iterator<Text> iterator = values.iterator();
			while (iterator.hasNext())
				context.write(iterator.next(), key);
		} // end reduce function
	} // end reducer class
	
	public static class SortFloatComparator extends WritableComparator {
		 
		//Constructor.
		 
		public SortFloatComparator() {
			super(FloatWritable.class, true);
		}
		
		@SuppressWarnings("rawtypes")
	 
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			FloatWritable k1 = (FloatWritable)w1;
			FloatWritable k2 = (FloatWritable)w2;
			
			return -1 * k1.compareTo(k2);
		}
	}

	public void sortPR(String inPath0, String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Configuration conf = new Configuration() ;
		
		Path pt = new Path(inPath0) ;
		FileSystem fs = FileSystem.get(new URI(inPath0), conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt))) ;
		String line = br.readLine() ;
		String numTitles = line.substring(2) ;
		conf.set("NUM_TITLES", numTitles) ;

		Job job = new Job(conf, "sortpr") ;
		job.setJarByClass(sortedPR.class);
		
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(sortPRMapper.class);
		job.setReducerClass(sortPRReducer.class) ;
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setSortComparatorClass(SortFloatComparator.class);
		
		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.waitForCompletion(true);
	}
} // end outermost class


