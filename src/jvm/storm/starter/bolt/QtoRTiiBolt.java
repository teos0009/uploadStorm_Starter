package storm.starter.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

//manipulate json
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
//manipulate redis via jedis
import redis.clients.jedis.Jedis;//add the jedis-2.1.0.jar to home/storm/lib; then manually add to project via project->properties->build path

public class QtoRTiiBolt extends BaseBasicBolt {

	Integer id;
	String name;
	Integer count;
	//Map<String, Integer> counters;
    //Map<String,String> keyval;//key value store; maybe val as array list than string will be better.
    //ArrayList<String> alTemp;
    //Map<String,ArrayList<String>>keyval2;//using arrlst s.t sort can be done on array list 
	Jedis jedis;
	String host = "localhost"; 
	int port = 6379;
    
	private void reconnect() {
		this.jedis = new Jedis(host, port);
	}//end reconnect

    //cleanup called when storm ends
	@Override
	public void cleanup() {
		System.out.println("--  ["+name+"-"+id+"] --");
/*		
		for(Map.Entry<String, Integer> entry : counters.entrySet()){
			System.out.println(entry.getKey()+": "+entry.getValue());
		}//end for

		System.out.println("keyval2  [" + name + "-" + id + "] --");
		for(Map.Entry<String, ArrayList<String>> kvPair2 : keyval2.entrySet()){
			alTemp = kvPair2.getValue();
			System.out.print(kvPair2.getKey()+": ");
			for( String strTemp:alTemp){
				System.out.print(strTemp);		
			}
		}//end for		
	*/	
	}//end cleanup

	/**
	 * On create 
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		//this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		//this.keyval = new HashMap<String, String>();//naive style no sort
		//this.keyval2 = new HashMap<String, ArrayList<String>>();//sort by tf
		this.count = 1;
		reconnect();//connect to redis
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}//nothing going to emit, so juz leave blank.

//item from redisQ inster to  RTII
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);
       System.out.println("item received from redis Q " + str);
       insertRTII(str);
       
       
	}//end execute
	
	public void insertRTII(String txt){
		String [] terms = txt.split("\\s+"); 
		
		 txt = terms[0] + " " + terms[1]+ " " + count;
		jedis.zadd(terms[2], count, txt);//
		count++;
		}//end insertRedis	
}//end class