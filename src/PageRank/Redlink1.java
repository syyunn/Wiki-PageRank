package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Redlink1 {
	
	public static class Redlink1Mapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int titleTabIndex = value.find("\t") ;
			

			if (titleTabIndex == -1) {
				
				context.write(new Text(value.toString()), new Text("!")) ;
			} 
			else { 
				String title = Text.decode(value.getBytes(),0,titleTabIndex) ;
				Text keyTitle = new Text(title) ;
				context.write(keyTitle, new Text("!"));
				String links = Text.decode(value.getBytes(), titleTabIndex+1, value.getLength()-(titleTabIndex+1)) ;
				String[] allLinks = links.split("\t") ;

				for (String otherLink: allLinks){
					otherLink = otherLink.trim() ;
					if (otherLink != null)
						if (!otherLink.isEmpty())
							context.write(new Text(otherLink), keyTitle);
				} // end for
			} // end else
			
		} // end map function
		
	} // end mapper class
	
	public static class Redlink1Reducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String valMarkerOrLinksOrRank ;
			String allLinks = "" ; //= new String() ;
			boolean existingPageFlag = false ;
			boolean first = true ;
			Iterator<Text> iterator = values.iterator() ;
			while(iterator.hasNext()) {
				valMarkerOrLinksOrRank = iterator.next().toString() ;
				
				if (valMarkerOrLinksOrRank.equals("!")) 
					existingPageFlag = true ;
				else {
					if (!first)
						allLinks = allLinks+"\t"+valMarkerOrLinksOrRank ;
					else {
						allLinks = allLinks+valMarkerOrLinksOrRank ;
						first = false ;
					}
				} // end else
				
			} // end while
			if (existingPageFlag) 
				context.write(key, new Text(allLinks)) ;
		} // end reduce function
	} // end reducer class

	public void runRedlinkRemover1(String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration() ;
		
		Job job = new Job(conf, "redlink1") ;
		job.setJarByClass(Redlink1.class);

		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	        
	    job.setMapperClass(Redlink1Mapper.class);
	    job.setReducerClass(Redlink1Reducer.class) ;
	    	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(inPath));
	    FileOutputFormat.setOutputPath(job, new Path(outPath));
	        
	    job.waitForCompletion(true);
	}
} // end outermost class

