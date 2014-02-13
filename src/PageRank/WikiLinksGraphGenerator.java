package PageRank;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.LinkedHashSet;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import PageRank.XmlInputFormat;



public class WikiLinksGraphGenerator {

	public  static class WikiLinksMapper extends Mapper<LongWritable, Text, Text, Text> {

		private static final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]") ;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int start = value.find("<title>") ;
			int end = value.find("</title>", start) ;
			if (start == -1 || end == -1) return ;
			start += 7 ;
			
			
			String titlePortion = Text.decode(value.getBytes(), start, end-start) ;
			/*// check whether titlePortion is a valid string
			if (titlePortion.contains(":"))
				return ;*/
			// if it valid 
			titlePortion = titlePortion.replace(' ', '_') ;
			Text keyPage = new Text(titlePortion) ;
			
			String outLinkVals = "" ;

				start = value.find("<text") ; 
				if (start == -1) {
					context.write(keyPage, new Text(outLinkVals));
					return ;
				}
				start = value.find(">", start) ;
				if (start == -1) {
					context.write(keyPage, new Text(outLinkVals));
					return ;
				}
				end = value.find("</text>") ; 
				if (end == -1) {
					context.write(keyPage, new Text(outLinkVals));
					return ;
				}
				start += 1 ;

				String textPortion = Text.decode(value.getBytes(), start, end-start) ;

				

				// now extract links from the textPortion
				Matcher wikiLinksMatcher = wikiLinksPattern.matcher(textPortion) ;


				LinkedList<String> duplicateList = new LinkedList<String>()  ;
				while (wikiLinksMatcher.find()){
					String outLinkPage = wikiLinksMatcher.group() ;
					outLinkPage = getWikiPageFromLink(outLinkPage) ;

					if (outLinkPage != null){
						if (!outLinkPage.isEmpty()){
							outLinkPage = outLinkPage.trim() ;
							duplicateList.add(outLinkPage) ;
						}
					}

							} // end while


				LinkedHashSet<String> listToSet = new LinkedHashSet<String>(duplicateList) ;
				LinkedList<String> listWithoutDuplicates = new LinkedList<String>(listToSet) ;

				
				boolean first = true ; 
				for (String vals: listWithoutDuplicates){
					if (!vals.equals(titlePortion)) {
						if (!first) 
							outLinkVals += "\t" ;
						outLinkVals += vals ;
						first = false ;
					}
				}


				context.write(keyPage, new Text(outLinkVals));
			
		} // end map function

		private String getWikiPageFromLink(String _outLinkPage){
			if (isNotWikiLink(_outLinkPage)) return null ;

			int start = 1 ;
			if(_outLinkPage.startsWith("[["))
				start = 2 ;

			int end = _outLinkPage.indexOf("]") ;

			int pipePosition = _outLinkPage.indexOf("|") ;
			if (pipePosition > 0){
				end = pipePosition ;
			}
			_outLinkPage = _outLinkPage.substring(start, end) ;
			_outLinkPage = _outLinkPage.replaceAll("\\s", "_") ;

			return _outLinkPage ;
		} // end getWikiPageFromLink function

		private boolean isNotWikiLink(String _outLinkPage) {
			int start = 1;
			if(_outLinkPage.startsWith("[[")){
				start = 2;
			}

			if( _outLinkPage.length() < start+2 || _outLinkPage.length() > 100) return true;
			char firstChar = _outLinkPage.charAt(start);

			switch(firstChar){
			case '#': return true;
			case ',': return true;
			case '.': return true;
			case '&': return true;
			case '\'':return true;
			case '-': return true;
			case '{': return true;
			}	        
			if( _outLinkPage.contains(":")) return true; // Matches: external links and translations links
			if( _outLinkPage.contains(",")) return true; // Matches: external links and translations links
			if( _outLinkPage.contains("&")) return true;

			return false;
		}

	} // end Mapper class


	/*public static class WikiLinksReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String pageRank = "1.0\t" ;

			boolean first = true ;
			for (Text val : values){
				if (!first) pageRank += "," ;
				pageRank += val.toString() ;
				first = false ;
			} // end for

			context.write(key, new Text(pageRank)) ;

		}

	} // end reducer class
	 */	
	public void parseXML(String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration() ;
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>") ;
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>") ;

		Job job = new Job(conf, "pagerank") ;
		job.setJarByClass(WikiLinksGraphGenerator.class);

		// Mapper
		FileInputFormat.addInputPath(job, new Path(inPath)) ;
		// job.setInputFormatClass(XmlInputFormat.class) ;
		job.setInputFormatClass(XmlInputFormat.class) ;
		job.setMapperClass(WikiLinksMapper.class) ;

		// Reducer
		FileOutputFormat.setOutputPath(job, new Path(outPath)) ;
		job.setOutputFormatClass(TextOutputFormat.class) ;
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// job.setReducerClass(WikiLinksReducer.class);
		job.waitForCompletion(true); 
		
	

	}

} // end topmost class
