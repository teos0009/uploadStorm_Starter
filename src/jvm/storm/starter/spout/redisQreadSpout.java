package storm.starter.spout;

import backtype.storm.utils.Utils;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
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




//manipulate redis via jedis
import redis.clients.jedis.Jedis;//add the jedis-2.1.0.jar to home/storm/lib; then manually add to project via project->properties->build path
//log4j
import org.apache.log4j.Logger;


public class redisQreadSpout extends BaseRichSpout {	
	static Logger LOG = Logger.getLogger(redisQreadSpout.class);
	private SpoutOutputCollector collector;
	//String host = "localhost"; //debug on localhost
	String host = "ec2-54-224-95-128.compute-1.amazonaws.com";//aws_redis public url
	int port = 6379;
	//Queue<String> msgQ = new LinkedList<String>();
	static Queue<String> msgQ = new LinkedList<String>();//need to be static var.
    String queues = "myfifo";//name of my redis q
	
	public void ack(Object msgId) {
		System.out.println("OK:"+msgId);
	}//end ACK
	
	public void close() {}
	
	public void fail(Object msgId) {
		System.out.println("FAIL:"+msgId);
	}//end fail
	
	//first menthod called in any spout; rx topo context; spoutoutputcollector is to emit data will be processed by bolts 
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
			new Thread(new Runnable() {
	            @Override
	            public void run() {
	                while(true){
	                    try{
	                        Jedis client = new Jedis(host, port);
	                        List<String> res = new ArrayList<String>();
	                        		res = client.blpop(Integer.MAX_VALUE, queues);//blpop does not block IO as expected without checking cond.
	                        		
	                        		
	                        //shin: weird error of null pointer error caused by res. commented these code the redis Q manage to deque all items 
	                        //bplot will return nil when no item in Q		
	                        		
	                        if(res == null){
	                        	System.out.println("res is empty Q");
	                        	continue;
	                        }
	                        	                        		
	                        else if(!res.isEmpty()){
	                        	System.out.println("the res=" +res.toString() + " | get(1) " + res.get(1)+ " | get(0) " + res.get(0));
	                                          
	                        
	                        msgQ.offer(res.get(1));//insert to jvm Q with confidence from redis Q//weird null pointer exception here
	                        //msgQ.offer("the REDIS Q tix");//debug only: to chk bolt can receive data
	                        }
	                    }catch(Exception e){
	                        LOG.error("Error reading queues from redis",e);
	                        try {
	                            Thread.sleep(100);
	                        }
	                        catch (InterruptedException e1) {}
	                    }//end catch
	                }//end while
	            }//end run
	     }//end thread
			).start();
			
			this.collector = collector;
	}//end open


	/**
      blocking call to read Q data struct on redis. Q contains real time incoming data from chrome extension
	 */
	//emit values to be processed by the bolts; this case emit 1 line as the value" 
	public void nextTuple() {
		try{
			while(!msgQ.isEmpty()){
				System.out.println("inside nextTuple");
				String theQtix = msgQ.poll();
				if(!theQtix.isEmpty()){
			    System.out.println("Q not empty");
	            //collector.emit(new Values(msgQ.poll()));//remove head of Q| null ptr exception here
			    collector.emit(new Values(theQtix));
				}
	        }
		}//end try
		catch(Exception e){
			throw new RuntimeException("Error redis Q",e);
		}
	}//end nextTuple

	//declare the type of var emitted
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("qtix"));//item to be parsed by inverted index bolt
	}
}