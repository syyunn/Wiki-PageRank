package PageRank;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import PageRank.WikiLinksGraphGenerator;

public class PageRank {
	
	public static String DATA_SET = "s3://spring-2014-ds/data/enwiki-latest-pages-articles.xml";

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		// TODO Auto-generated method stub
		
		String bucketName = args[0];
		String arg0 = DATA_SET;
		String arg1 = "s3://"+bucketName+"/tmp/1";
		String arg2 = "s3://"+bucketName+"/tmp/2";
		String arg3 = "s3://"+bucketName+"/tmp/3";
		String arg4 = "s3://"+bucketName+"/tmp/4";
		String arg5 = "s3://"+bucketName+"/tmp/5";
		String arg6 = "s3://"+bucketName+"/tmp/6";
		String arg7 = "s3://"+bucketName+"/tmp/7";
		String arg8 = "s3://"+bucketName+"/tmp/8";

		String outLink = "s3://"+bucketName+"/results/PageRank.outlink.out";
		String outLink1 = "s3://"+bucketName+"/results/PageRank.n.out";
		String outLink2 = "s3://"+bucketName+"/results/PageRank.iter1.out";
		String outLink3 = "s3://"+bucketName+"/results/PageRank.iter8.out";
		
		WikiLinksGraphGenerator graphGen = new WikiLinksGraphGenerator() ;
		graphGen.parseXML(arg0, arg1);
		
		Redlink1 redlink1 = new Redlink1() ;
		redlink1.runRedlinkRemover1(arg1, arg5); // converts to inlink graph
		redlink1.runRedlinkRemover1(arg5, arg6); // converts inlink graph back to outlink graph
		
		CountN counter = new CountN() ;
		counter.countNumLines(arg6, arg2);
		
		PRIter0Graph iter0Graph = new PRIter0Graph() ;
		
		String path0 = arg2+"/part-r-00000" ;
		iter0Graph.generatePRIter0Graph(path0, arg6, arg3);
		
		PRCalculator prcalc = new PRCalculator() ;
		prcalc.runPRCalculator(arg3, arg4+"1");
		for(int run = 1; run<8;run++)
			prcalc.runPRCalculator(arg4+run, arg4+(run+1));
		
		Configuration conf = new Configuration() ;

		FileSystem fs;
		 fs = FileSystem.get(new URI("s3://"+bucketName), conf);
          Path src = new Path(arg6);
          Path dst = new Path(outLink);
          FileUtil.copyMerge(fs, src, fs, dst, false, conf, "");
          
          Path src1 = new Path(arg2);
          Path dst1 = new Path(outLink1);
          FileUtil.copyMerge(fs, src1, fs, dst1, false, conf, "");
          
        
          
                  
          sortedPR prsorter = new sortedPR() ;
  		prsorter.sortPR(path0, arg4+"1", arg7);
  		prsorter.sortPR(path0, arg4+"8", arg8);
  		
  	  Path src2 = new Path(arg7);
      Path dst2 = new Path(outLink2);
      FileUtil.copyMerge(fs, src2, fs, dst2, false, conf, "");
      
      Path src3 = new Path(arg8);
      Path dst3 = new Path(outLink3);
      FileUtil.copyMerge(fs, src3, fs, dst3, false, conf, "");



	}

}
