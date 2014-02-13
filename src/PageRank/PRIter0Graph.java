package PageRank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PRIter0Graph {

	public static class PRIter0Mapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration() ;
			long numTitles = Long.parseLong(conf.get("NUM_TITLES")) ;   // Integer.parseInt(NUM_TITLES) ;
			float initialRank = (float) (1.0/numTitles) ;

			int titleIndex = value.find("\t") ;
			String title = Text.decode(value.getBytes(),0,titleIndex) ;
			String outLinks = Text.decode(value.getBytes(),titleIndex+1, value.getLength()-(titleIndex+1)) ;
			context.write(new Text(title+"\t"+initialRank), new Text(outLinks));

		}
	} // end mapper class

	public void generatePRIter0Graph(String inPath0, String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration() ;

		Path pt = new Path(inPath0) ;
		FileSystem fs = FileSystem.get(new URI(inPath0), conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt))) ;
		String line = br.readLine() ;
		String numTitles = line.substring(2) ;
		conf.set("NUM_TITLES", numTitles) ;

		Job job = new Job(conf, "iter0Graph") ;
		job.setJarByClass(PRIter0Graph.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(PRIter0Mapper.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.waitForCompletion(true);

	}

}
