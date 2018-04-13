import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

public class WikiStreamer{
	
	public static void main(String[] args) {
		RetrieveWikiData();
	}
	
	public static int RetrieveWikiData() {
		/// Parameters for URL...
		String project = "en.wikipedia.org";
		String access = "all-access";
		String agent = "user";
		String article = "albert_einstein,doctor_who,hummingbird,australia,buzz_aldrin,nasa,london,wasp";
		String articles[] = article.split(",");
		String granularity = "daily";
		String start = "20180101";
		String end = "20180201";
		OutputStream outStream = null;
		
		/// For writing output of each Get call...
		String outputFile = "d:\\wikitest.txt";
		File targetFile = new File(outputFile);
		
		try {
			outStream = new FileOutputStream(targetFile);
			
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
				IOUtils.copy(input, outStream, input.available());
				httpClient.close();
			}
			
			outStream.close();
			return 1;
		}catch(Exception ex) {
			System.out.print(ex.getMessage());
			return 0;
		}finally {
			if(outStream != null) {
				try {
					outStream.close();
				}catch (IOException e) {
					System.out.print(e.getMessage());
				}
			}else {
				System.out.print("OutputStream already closed");
			}
		}
		
	}
}
