package storm.starter.spout;

import backtype.storm.utils.Utils;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;
import java.util.Queue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

//////using org.json.simple in storm starter
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


//===http retrieve=====
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;


//manipulate redis via jedis
import redis.clients.jedis.Jedis;//add the jedis-2.1.0.jar to home/storm/lib; then manually add to project via project->properties->build path
//log4j
import org.apache.log4j.Logger;


public class consumeDataGovSpout extends BaseRichSpout {	
	static Logger LOG = Logger.getLogger(redisQreadSpout.class);
	private SpoutOutputCollector collector;
	String host = "localhost"; //debug on localhost
	//String host = "ec2-54-224-95-128.compute-1.amazonaws.com";//aws_redis public url
	int port = 6379;
	
	BufferedReader reader = null;
	
	
	//Queue<String> msgQ = new LinkedList<String>();
	
	/*
	static Queue<String> msgQ = new LinkedList<String>();//need to be static var.
    String queues = "myfifo";//name of my redis q
	*/
	public void ack(Object msgId) {
		System.out.println("OK:"+msgId);
	}//end ACK
	
	public void close() {}
	
	public void fail(Object msgId) {
		System.out.println("FAIL:"+msgId);
	}//end fail
	
	//first menthod called in any spout; rx topo context; spoutoutputcollector is to emit data will be processed by bolts 
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector)  {
//		//put open stream here got error: stream closed		
//		try {
//			URL url = new URL("http://developer.usa.gov/1usagov");
//			InputStreamReader myInputStream = new InputStreamReader (url.openStream(), "UTF-8");
//			reader = new BufferedReader(myInputStream);
//		} catch (MalformedURLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}//pubsub style=http://developer.usa.gov/1usagov
//          catch (UnsupportedEncodingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

			this.collector = collector;
	}//end open


	/**
      blocking call to read Q data struct on redis. Q contains real time incoming data from chrome extension
	 */
	//emit values to be processed by the bolts; this case emit 1 line as the value" 
	public void nextTuple() {//json can be extracted field of interest or pass as a whole to redis
		try{
			
			// TODO Auto-generated method stub
			URL url = new URL("http://developer.usa.gov/1usagov");//move to open()
		
			try {
				InputStreamReader myInputStream = new InputStreamReader (url.openStream(), "UTF-8");//move to open()
				reader = new BufferedReader(myInputStream);//move to open()

			    for (String line; (line = reader.readLine()) != null;) 
			    {
			    	//System.out.println(line);//debug only 
			    	StringBuilder  tempStr = new StringBuilder();
			    	if(line.length() > 0){//first value sent is null, so if parse direct will have error
			    	tempStr.append(line);
			    	Object objV=JSONValue.parse(tempStr.toString());//OK to parse json text to obj
			    	//System.out.println(objV);
			    	JSONObject simpleJsonObject = (JSONObject) objV;
			    	if (line.length() > 5){//only clicks, not heartbeat
			    	//System.out.println(simpleJsonObject.get("g")+" "+simpleJsonObject.get("u"));//debug only
			    	//assume country is dictionary, posting list is url, time stamp etc. Can be customized to suit job needed
	 		    	if(simpleJsonObject.get("c")!=null){
	 		    		
			    	StringBuilder country = new StringBuilder();
			    	StringBuilder posting = new StringBuilder();
			    	country.append(simpleJsonObject.get("c"));//country
			    	
			    	//depends on the task, posting need to customized to it. 2 style.
			    	
			    	//style1:posting contains field of interest : hash, url, time, city name. json can be stored whole 
//			    	posting.append(simpleJsonObject.get("h"));//hash
//			    	posting.append(" ");
			    	posting.append(simpleJsonObject.get("u"));//url
//			    	posting.append(" ");
//			     	posting.append(simpleJsonObject.get("t"));//time
//			     	posting.append(" ");
//			     	posting.append(simpleJsonObject.get("cy"));//cityname
			     	
//			     	//style2:posting contains json object as a whole
//			     	posting.append(simpleJsonObject.toJSONString());
			     	
			     	String countryName = country.toString();
			     	String postItem = posting.toString();
			     	
			     	this.collector.emit(new Values(countryName,postItem));//emit as <country, posting> 
	 		    	}//if c not null
			    	}//if not heart beat
			    	}//end if
			    	
		    }
			} 
			
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
		}//end try
		catch(Exception e){
			throw new RuntimeException("Error next tuple",e);
		}
	}//end nextTuple

	//declare the type of var emitted
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("country","post"));//emit tuple as <country, postinglist>
		//assume country is dictionary, posting list is url, time stamp etc.
	}
}