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

public class PRCalculator {

	public static class PRCalcMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int titleTabIndex = value.find("\t") ;
			String title = Text.decode(value.getBytes(),0,titleTabIndex) ;
			context.write(new Text(title), new Text("!")) ;

			int rankTabIndex = value.find("\t", titleTabIndex+1) ;

			if (rankTabIndex != -1) {
				String links = Text.decode(value.getBytes(), rankTabIndex+1, value.getLength()-(rankTabIndex+1)) ;
				context.write(new Text(title), new Text("|"+links));

				float currentRank = Float.parseFloat(Text.decode(value.getBytes(), titleTabIndex+1, rankTabIndex-(titleTabIndex+1))) ;
				String[] allLinks = links.split("\t") ;
				int numLinks = allLinks.length ;

				float rankContributed = currentRank/numLinks ;
				Text valRank = new Text(String.valueOf(rankContributed)) ; 

				for (String otherLink: allLinks){
					otherLink = otherLink.trim() ;
					if (otherLink != null)
						if (!otherLink.isEmpty())
							context.write(new Text(otherLink), valRank);
				} // end for
			} // end if
		} // end map function

	} // end mapper class

	public static class PRCalcReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String valMarkerOrLinksOrRank ;
			String allLinks = "" ; //= new String() ;
			float sumOfRankShares = 0 ;
			boolean existingPageFlag = false ;
			Iterator<Text> iterator = values.iterator() ;
			while(iterator.hasNext()) {
				valMarkerOrLinksOrRank = iterator.next().toString() ;

				if (valMarkerOrLinksOrRank.equals("!")) 
					existingPageFlag = true ;
				else if (valMarkerOrLinksOrRank.startsWith("|")) 
					allLinks = valMarkerOrLinksOrRank.substring(1) ;
				else
					sumOfRankShares += Float.parseFloat(valMarkerOrLinksOrRank) ;
			} // end while
			if (existingPageFlag) 
				context.write(key, new Text(sumOfRankShares+"\t"+allLinks)) ;
		} // end reduce function
	} // end reducer class

	public void runPRCalculator(String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration() ;

		Job job = new Job(conf, "prcalculator") ;
		job.setJarByClass(PRCalculator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(PRCalcMapper.class);
		job.setReducerClass(PRCalcReducer.class) ;

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.waitForCompletion(true);
	}
} // end outermost class
