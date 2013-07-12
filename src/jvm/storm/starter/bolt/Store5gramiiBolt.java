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

//this is the bolt that will store bigram inverted index into redis

/*
public class Store5gramiiBolt {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}
}
*/

public class Store5gramiiBolt extends BaseBasicBolt {

	Integer id;
	String name;
	Map<String, Integer> counters;
    Map<String,String> keyval;//key value store; maybe val as array list than string will be better.
    ArrayList<String> alTemp;
    Map<String,ArrayList<String>>keyval2;//using arrlst s.t sort can be done on array list 
	Jedis jedis;
	String host = "localhost"; 
	int port = 6379;
    
	private void reconnect() {
		this.jedis = new Jedis(host, port);
	}//end reconnect

		/**
	 * At the end of the spout (when the cluster is shutdown
	 * We will show the word counters
	 */
    //cleanup called when storm ends
	@Override
	public void cleanup() {
		System.out.println("--  ["+name+"-"+id+"] --");
		for(Map.Entry<String, Integer> entry : counters.entrySet()){
			System.out.println(entry.getKey()+": "+entry.getValue());
		}//end for
		/*
		System.out.println("keyval  [" + name + "-" + id + "] --");
		for(Map.Entry<String, String> kvPair : keyval.entrySet()){
			System.out.println(kvPair.getKey()+": "+kvPair.getValue());
		}//end for		
		*/
		System.out.println("keyval2  [" + name + "-" + id + "] --");
		for(Map.Entry<String, ArrayList<String>> kvPair2 : keyval2.entrySet()){
			alTemp = kvPair2.getValue();
			System.out.print(kvPair2.getKey()+": ");
			for( String strTemp:alTemp){
				System.out.print(strTemp);		
			}
			System.out.println("***");
		}//end for		
		
	}//end cleanup

	/**
	 * On create 
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		this.keyval = new HashMap<String, String>();//naive style no sort
		this.keyval2 = new HashMap<String, ArrayList<String>>();//sort by tf
		reconnect();//connect to redis
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}//nothing going to emit, so juz leave blank.

//bigram inverted index the reducer equivalent
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);
		String txt = input.getStringByField("txt");
		String bigram = input.getStringByField("bigram");
		incr(str);//increase counter
		//insertKeyVal(bigram,txt);//insert to keyval store via hash map
		insertKeyVal2(bigram,txt);//insert to keyval2 Map<String,ArrayList<String>>
		
		//pump to redis via jedis
		insertRedis(bigram,txt);
		
		 //If the word dosn't exist in the map we will create
		 //this, if not We will add 1 
		 
		System.out.println("tuple<bigram,tf|txt>= "+bigram + " | "+txt);
		//strategy: shard by bigram, within each shard, sort by tf for retrieval of popular key words
		//txt format: tf w1 w2 w3; bigram format: w1 w2
		
		/*
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		}else{
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
		*/
	}//end execute
	
	public void incr(String str){
		
		/**
		 * If the word dosn't exist in the map we will create
		 * this, if not We will add 1 
		 */
		
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		}else{
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
		
	}//end icr
	
	//insert to key val store (hashmap now, redis later) via strategy desc above.
	public void insertKeyVal(String bigram, String txt){
		if(!keyval.containsKey(bigram)){
			keyval.put(bigram, "|"+txt);//new entry "|" is the marker vs OOP method
		}else{
			//dup key, arrange by tf
			//complicated loop of manipulating string params. need more elegant soln
			String temp = keyval.get(bigram);
			
			 //sort by tf style via string
			//String [] terms = temp.split("\\s+"); 

			//no sort style
			temp = temp + "|" +txt;
			keyval.put(bigram, temp);
		 
		}//end else

	}//end insert
	
	//hashmap<string,ArrayList<String>> style
	public void insertKeyVal2(String bigram, String txt){
		ArrayList<String> lst = new ArrayList<String>();
		if(!keyval2.containsKey(bigram)){
			lst.add(txt);
			keyval2.put(bigram, lst);
			
		}else{
         lst = keyval2.get(bigram);
		 lst.add(txt);
		 Collections.sort(lst,Collections.reverseOrder());//sort descending via tf
		 keyval2.put(bigram, lst);
		}//end else
		
	}//end insertKeyVal2
	
	
	public void insertRedis(String bigram, String txt){
		String [] terms = txt.split("\\s+"); 
		double tf = Double.parseDouble(terms[0]);
		//txt = txt.substring(1, txt.length());//remove tf from text data
		 txt = terms[1] + " " + terms[2]+ " " + terms[3];
		//bigram as key, array list as value in string style
		//jedis.set(bigram, txt);//same keys but val diff, only the last value will be saved
		
		//jedis.zadd(key, score, member);//sorted set style
		
		jedis.zadd(bigram, tf, txt);//same key name in redis cannot be re-used for diff type of data struct
		//ZREVRANGE "the legend" 0 10 return top11 result sorted desc by tf

		}//end insertRedis	
	
}//end class