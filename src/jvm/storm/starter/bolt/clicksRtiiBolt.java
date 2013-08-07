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

public class clicksRtiiBolt extends BaseBasicBolt {

	Integer id;
	String name;
	Integer count;
	Jedis jedis;
	//String host = "localhost"; //debug local host
	String host = "ec2-54-227-53-76.compute-1.amazonaws.com";//aws redis
	int port = 6379;
    
	private void reconnect() {
		this.jedis = new Jedis(host, port);
	}//end reconnect

    //cleanup called when storm ends
	@Override
	public void cleanup() {
		System.out.println("cleanup");
		System.out.println("--  ["+name+"-"+id+"] --");

	}//end cleanup

	/**
	 * On create 
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		//this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		this.count = 1;
		reconnect();//connect to redis
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}//nothing going to emit, so juz leave blank.

//item from redisQ inster to  RTII
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str0 = input.getString(0);
		String str1 = input.getString(1);
		String txt = input.getStringByField("country");//from spout declareOutputFields
		String posting = input.getStringByField("post");
       System.out.println("txt: "+txt+" "+"posting:"+posting);
       insertUrlVisitedByCountry(txt,posting);
       
       
	}//end execute
	
	public void insertUrlVisitedByCountry(String txt, String posting){
//depends on the task requred: choose redis data struct and param to insert
//eg url visited by a country. the set is country name, inside contains url.
		//first visit in url, insert, duplicated, increase the score of the insert via zadd
		long val = jedis.zadd(txt, 1, posting);//add sorted set<txt, score, posting>
        if(val == 0){
        	double score = 0;
        	score = jedis.zscore(txt, posting);
        	score = score + 1;//dup url in same country increase the score/count
        	jedis.zincrby(txt, score, posting);
        	
        }
		
		}//end insertRedis	
}//end class