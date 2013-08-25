package test1;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.net.HttpURLConnection;
import java.net.URL;

//////using org.json.simple in storm starter
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


//apache httpclient
import org.apache.commons.codec.binary.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;

public class myHTTPSpam {
	
/*
 * 
[
{ "a": "Mozilla\/5.0 (Linux; U; Android 4.0.4; en-us; SCH-S738C Build\/IMM76D) AppleWebKit\/534.30 (KHTML, like Gecko) Version\/4.0 Mobile Safari\/534.30", "c": "US", "nk": 0, "tz": "America\/New_York", "gr": "PA", "g": "1bVN4Tk", "h": "15KmpUu", "l": "o_41uiphapgu", "al": "en-US", "hh": "1.usa.gov", "r": "http:\/\/m.facebook.com\/l.php?u=http%3A%2F%2F1.usa.gov%2F15KmpUu&h=9AQF-Ei_K&enc=AZP4lQ2dmFp8LeIKquGeoZxvzwU48OP4aaY_J6FiNJCOBE47XIHP3E0iQqez13qaaKjW9f46ttlLqqYsB076n7PDl_b7mXUk5TMNNXp6K2SWFjj-CiqvYfVwtuFN6Tw9HRsPup771L3Iw2tZqIVHxF1_&s=1", "u": "http:\/\/science.nasa.gov\/science-news\/science-at-nasa\/2013\/26jul_perseids\/", "t": 1375916400, "hc": 1375793954, "cy": "East Berlin", "ll": [ 39.970699, -77.010498 ] }
,
{ "a": "Mozilla\/5.0 (Macintosh; Intel Mac OS X 10.6; rv:22.0) Gecko\/20100101 Firefox\/22.0", "c": "NZ", "nk": 0, "tz": "Pacific\/Auckland", "gr": "E9", "g": "14edieX", "h": "1c71Dob", "l": "o_41uiphapgu", "al": "en-US,en;q=0.5", "hh": "1.usa.gov", "r": "http:\/\/www.facebook.com\/l.php?u=http%3A%2F%2F1.usa.gov%2F1c71Dob&h=RAQHZ9fPR&s=1", "u": "http:\/\/photojournal.jpl.nasa.gov\/catalog\/PIA17041", "t": 1375874003, "hc": 1375793529, "cy": "Christchurch", "ll": [ -43.533298, 172.633301 ] }
]
//add openclose square bracket at start and end. after each } need a comma.l
 * */	
	

	/**
	 * @param args
	 * @throws MalformedURLException 
	 */
	private final String USER_AGENT = "Mozilla/5.0";
	//click json source = /home/user/Downloads/clicksCollec/datagov_consumer.log.2013-08-08 //141832 clicks collected in 24 hours
	public static void main(String[] args) throws MalformedURLException {
		// TODO Auto-generated method stub
		URL url = new URL("http://developer.usa.gov/1usagov");//pubsub style=http://developer.usa.gov/1usagov
		String path = "/home/user/Downloads/clicksCollec/edt.datagov_consumer.log.2013-08-08";//139172 unique clicks
		String urlBase = "http://ec2-50-16-146-29.compute-1.amazonaws.com:7379";//the correct url to insert to redis directly via webdis lpush to be read by readQ
		//String urlBase = "http://ec2-54-211-52-81.compute-1.amazonaws.com";//aws_redis gives 403
		//String urlBase = "http://ec2-174-129-142-174.compute-1.amazonaws.com";//aws nimbus gives 200
		//webdis url style: http://ec2-54-211-52-81.compute-1.amazonaws.com:7379/lpush/myfifo/item1234%20descriptionABCD%20KKKK
		//need [ ] and "," after each json obj

		//String path = "/home/user/Downloads/clicksCollec/consumer2";
		
		int counter = 0;
		BufferedReader reader = null;
		JSONParser parser = new JSONParser();
		
		long startTime = System.nanoTime();
		
		
		try {
			
			/*
			////apache httpclient style
			//HttpClientExample http = new HttpClientExample();
			myHTTPSpam myHttp = new myHTTPSpam();
			//System.out.println("Testing 1 - Send Http GET request");
			//myHttp.sendGet();
			Thread.currentThread();
			//for ( int i = 0; i < 99; i ++){
			//Thread.sleep(10);
			System.out.println("\nTesting 2 - Send Http POST request ");
			//myHttp.sendPost();
			String params = "lpush/myfifo/item1234 descriptionABCD HHHH";//use%20 as spacer or not is ok, need cmd infront
			myHttp.sendPostJson(urlBase, params);
			//myHttp.javaPostJson(urlBase, params);
		    //}
			*/
			
			/*
			////debug Unexpected token VALUE SOLVED
		    int n = 32626319;
		    //some "hc" val terminated at next like. file consumer2 Unexpected token VALUE(60946) at position 13962880|13968020
		    //on 0808 file 32626319
			//test read json file
			InputStream is = new FileInputStream(path);
			BufferedReader readerFile = new BufferedReader(new InputStreamReader(is,"UTF-8"));
			readerFile.skip(n); // chars to skip
			System.out.println(readerFile.readLine());
			//"hc": 13748 |trucated| 60946, "cy": "Washington", "ll": [ 38.893299, -77.014603 ] }
			*/
			
			
			//===http portion====
			myHTTPSpam myHttp = new myHTTPSpam();
			
			//=======
			//json works from json file to json obj to thread
			JSONArray arrJson = (JSONArray) parser.parse(new FileReader(path));
			for (Object objJson : arrJson){
				
				
				StringBuilder tempStr= new StringBuilder();
				
			    JSONObject myObj = (JSONObject) objJson;
			    
			    if(myObj.toString().length()>30){
			    String gloHash = (String) myObj.get("g");
			    //System.out.println(gloHash);

			    String country = (String) myObj.get("c");
			    //System.out.println(country);

			    long time = (Long) myObj.get("t");//time is long
			    
			   
			    tempStr.append(gloHash);
			    tempStr.append(" ");
			    tempStr.append(time);
			    tempStr.append(" ");
			    tempStr.append(country);
			    tempStr.append(" ");
			    //item1234%20descriptionABCD%20EEEE
			    //glohash time country
			    
			    ////testing array in an array only
//			    JSONArray latlon = (JSONArray) myObj.get("ll");
//			    if(latlon !=null){//need to check data avail if not will null pointer
//			    for (Object objLl : latlon)
//			    {
//			      //System.out.println(objLl+" ");
//			      tempStr.append(objLl.toString());
//			    }//end for
//			    }//end if latlon
			    
			    Thread.currentThread();
				Thread.sleep(1);//eg sleep for 1000ms or 1sec, will take 139172seconds or 2319min or 38 hours to complete
				myHttp.sendPostJson(urlBase, tempStr.toString());
				System.out.println(tempStr.toString());//debug
			    
			    counter++;
			    }//end if not heart beat
			  }//end for
			long endTime = System.nanoTime();
			long duration = endTime - startTime;
			double seconds = (double)duration / 1000000000.0;
			System.out.println("total clicks="+counter +" "+"seconds taken: "+seconds);
		    //0ms thread sleep: total clicks=139172 seconds taken: 36.846654205
			//1ms thread sleep: total clicks=139172 seconds taken: 305.179392476
			//10ms thread sleep: total clicks=139172 seconds taken: 1550.905276726
			 //need to test on localhost redis jedis and webdis
			 
		}//end try 
		
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		finally {
		    if (reader != null) try { reader.close(); } catch (IOException ignore) {}
		}
		
	}//end main

	//apache httpclient style
	private void sendGet() throws Exception {
		 
		String url = "http://www.google.com/search?q=developer";
 
		HttpClient client = new DefaultHttpClient();
		HttpGet request = new HttpGet(url);
 
		// add request header
		request.addHeader("User-Agent", USER_AGENT);
 
		HttpResponse response = client.execute(request);
 
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + 
                       response.getStatusLine().getStatusCode());
 
		BufferedReader rd = new BufferedReader(
                       new InputStreamReader(response.getEntity().getContent()));
 
		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}
 
		System.out.println(result.toString());
 
	}//end http get
 
	// HTTP POST request
	private void sendPost() throws Exception {
 
		//String url = "https://selfsolve.apple.com/wcResults.do";
 
		String url = "http://requestb.in/104jq131";//requestbin site
		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost(url);
 
		// add header
		post.setHeader("User-Agent", USER_AGENT);
 
		
		//set parameters via http pos method
		List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
		urlParameters.add(new BasicNameValuePair("sn", "C02G8416DRJM"));
		//urlParameters.add(new BasicNameValuePair("cn", ""));
		//urlParameters.add(new BasicNameValuePair("locale", ""));
		//urlParameters.add(new BasicNameValuePair("caller", ""));
		urlParameters.add(new BasicNameValuePair("num", "12345"));
 
		post.setEntity(new UrlEncodedFormEntity(urlParameters));

		HttpResponse response = client.execute(post);
		
		
		System.out.println("\nSending 'POST' request to URL : " + url);
		System.out.println("Post parameters : " + post.getEntity());
		System.out.println("Response Code : " + 
                                    response.getStatusLine().getStatusCode());
 
		//read input stream and convert to string
		BufferedReader rd = new BufferedReader(
                        new InputStreamReader(response.getEntity().getContent()));
 
		BufferedReader pd = new BufferedReader(
                new InputStreamReader(post.getEntity().getContent()));
		
		StringBuffer result = new StringBuffer();
		StringBuilder postData = new StringBuilder();
		String line = "";
		String lineP = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}

		while ((lineP = pd.readLine()) != null) {
			postData.append(lineP);
		}
		System.out.println("result="+result.toString()+" postData= "+postData.toString());
 
	}//end http post
	
	// HTTP POST request
		private void sendPostJson(String urlBase, String params) throws Exception {
	 
			//webdis url style: http://ec2-54-211-52-81.compute-1.amazonaws.com:7379/lpush/myfifo/item1234%20descriptionABCD%20KKKK
	 
			//String url = urlBase+params;//weird got 403
			//System.out.println("the url:"+url);
			//String url = "http://ec2-54-211-52-81.compute-1.amazonaws.com:7379";
			String url = urlBase;
			HttpClient client = new DefaultHttpClient();
			//HttpPost post = new HttpPost("http://ec2-174-129-142-174.compute-1.amazonaws.com");//ret 200 works
			//HttpPost post = new HttpPost("http://ec2-54-211-52-81.compute-1.amazonaws.com:7379/lpush/myfifo/item1234%20descriptionABCD%20DDDD");//error 403
			//HttpPost post = new HttpPost("http://ec2-54-211-52-81.compute-1.amazonaws.com:7379");
			HttpPost post = new HttpPost(url);
			// add header
			post.setHeader("User-Agent", USER_AGENT);
	 
			
			////set parameters via http pos method
			//List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
			//urlParameters.add(new BasicNameValuePair("", "lpush"));//no good
			//urlParameters.add(new BasicNameValuePair("", "/myfifo/item1234%20descriptionABCD%20FFFF"));
			//urlParameters.add(new BasicNameValuePair("locale", ""));
			//urlParameters.add(new BasicNameValuePair("caller", ""));
			//urlParameters.add(new BasicNameValuePair("num", "12345"));
	        //String urlParameters = "lpush/myfifo/item1234%20descriptionABCD%20EEEE";//no good
			//post.setEntity(new UrlEncodedFormEntity(urlParameters));//default style
            
			////json style
			//JSONObject obj = new JSONObject();
			//obj.put("lpush", "/myfifo/item1234%20descriptionABCD%20DDDD");//unknown command result={"{\"lpush\":\"\\":[false,"ERR unknown command '{\"lpush\":\"\\'"]}
			//post.setEntity(new StringEntity(obj.toString(), "UTF-8"));
			
			//string object style works
            StringBuilder myStr = new StringBuilder();
            //myStr.append("lpush/myfifo/item1234%20descriptionABCD%20GGGG");//use this style
            myStr.append("lpush/myfifo/");//append cmd infront
            myStr.append(params);
            post.setEntity(new StringEntity(myStr.toString(), "UTF-8"));
			
			HttpResponse response = client.execute(post);
			
			
			System.out.println("\nSending 'POST' request to URL : " + url);
			System.out.println("Response Code : " + 
	                                    response.getStatusLine().getStatusCode());
	 
			/*
			 //not reading reply result
			//read input stream and convert to string
			BufferedReader rd = new BufferedReader(
	                        new InputStreamReader(response.getEntity().getContent()));
			
			StringBuffer result = new StringBuffer();
			String line = "";
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}

			System.out.println("result="+result.toString());
	 */
		}//end http post Json
//=============================================================
		
		@SuppressWarnings("null")
		private void javaPostJson(String urlBase, String params) throws Exception {//this style works
			//String theUrl = "http://ec2-54-211-52-81.compute-1.amazonaws.com:7379/lpush/myfifo/item1234%20descriptionABCD%20DDDD";
			String theUrl = "http://ec2-54-211-52-81.compute-1.amazonaws.com:7379";
			String url = theUrl;
			URL obj = new URL(url);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
	 
			//add reuqest header
			con.setRequestMethod("POST");
			con.setRequestProperty("User-Agent", USER_AGENT);
			con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
	 
			String urlParameters = "lpush/myfifo/item1234%20descriptionABCD%20DDDD";
	 
			// Send post request
			con.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(con.getOutputStream());
			wr.writeBytes(urlParameters);
			wr.flush();
			wr.close();
	 
			int responseCode = con.getResponseCode();
			System.out.println("\nSending 'POST' request to URL : " + url);
			System.out.println("Post parameters : " + urlParameters);
			System.out.println("Response Code : " + responseCode);
	 
			BufferedReader in = new BufferedReader(
			        new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();
	 
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
	 
			//print result
			System.out.println(response.toString());
			
		}//end java post json
	
}//end class
