import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;


public class WikiStreamerToHDFS {

	public static void main(String[] args) {
		RetrieveWikiData();
	}
	
	public static void RetrieveWikiData() {
		/// Parameters for URL...
		String project = "en.wikipedia.org";
		String access = "all-access";
		String agent = "user";
		String article = "albert_einstein,doctor_who,hummingbird,australia,buzz_aldrin,nasa,london,wasp";
		String articles[] = article.split(",");
		String granularity = "daily";
		String start = "20180101";
		String end = "20180201";
		System.setProperty("HADOOP_USER_NAME", "root");
		
		/// Hadoop file system objects...
		final Path path = new Path("/data/wikitest.txt");
		
		try { 
			final DistributedFileSystem dfs = new DistributedFileSystem() {
				{
					initialize(new URI("hdfs://hadoopmaster:54310"), new Configuration());
				}
			};
			final FSDataOutputStream streamWriter = dfs.create(path);
			
			for(int i = 0; i<articles.length; i++) {
				CloseableHttpClient httpClient = HttpClientBuilder.create().build();
	
				HttpGet httpGet = new HttpGet("https://wikimedia.org/api/rest_v1" +
											  "/metrics/pageviews/per-article/" + 
											  project + "/" + access + "/" + 
											  agent + "/" + articles[i] + "/" + 
											  granularity + "/" + start + "/" + end);
				
				HttpResponse response = httpClient.execute(httpGet);
				HttpEntity entity = response.getEntity();
				
				InputStream input = entity.getContent();
				
				final PrintWriter writer = new PrintWriter(streamWriter); 
				{
					String test = convertStreamToString(input);
					writer.println(test);
					writer.flush();
					System.out.println("PageView Results for " + articles[i] + " saved");
				}
				httpClient.close();
			}
			streamWriter.flush();
			streamWriter.close();
		} catch(Exception ex) {
			
		} finally
		{
		}
	}
	
	static String convertStreamToString(java.io.InputStream is) {
	    java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
	    return s.hasNext() ? s.next() : "";
	}
}
