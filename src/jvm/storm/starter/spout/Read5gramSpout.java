package storm.starter.spout;
//import backtype.storm.spout.SpoutOutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.topology.base.BaseRichSpout;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
//import java.util.Map;
//import java.util.Random;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


/*
public class Read5gramSpout {
	public static void main(String[] args) {
	}
}
*/
//base on wordreader.java
//method1 to process bigram inverted index with storm
public class Read5gramSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	public void ack(Object msgId) {
		System.out.println("OK:"+msgId);
	}
	public void close() {}
	public void fail(Object msgId) {
		System.out.println("FAIL:"+msgId);
	}

	/**
	 * The only thing that the methods will do It is emit each 
	 * file line
	 */
	//emit values to be processed by the bolts; this case emit 1 line as the value" 
	public void nextTuple() {
		/**
		 * The nextuple it is called forever, so if we have been read the file
		 * we will wait and then return
		 */
		if(completed){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//Do nothing
			}
			return;
		}
		String str;
		//Open the reader
		BufferedReader reader = new BufferedReader(fileReader);//var loaded at open()
		try{
			//Read all lines
			while((str = reader.readLine()) != null){
				/**
				 * By each line emit a new value with the line as a their
				 */
				
				//emit one line as valie. abit diff with MR stye <key, val>
				this.collector.emit(new Values(str),str);//book is-> emit(new Values(str));
				
				//!!!values is impl of ArrayList; elements of list passed to constructor
			}
		}catch(Exception e){
			throw new RuntimeException("Error reading tuple",e);
		}finally{
			completed = true;
		}
	}//end next tuple; called preriodically from ack() fail()//not written in this code, but can override as if randomsentencespout
	//tuple is named list of values java obj that is serializable
	

	/**
	 * We will create the file and get the collector object
	 */
	
	//first menthod called in any spout; rx topo context; spoutoutputcollector is to emit data will be processed by bolts 
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			
					
			this.fileReader = new FileReader(conf.get("wordsFile").toString());
			
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["+conf.get("thePath")+"]");
		}
		this.collector = collector;
	}

	/**
	 * Declare the output field "word"
	 */
	//declare the type of var emitted
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
}