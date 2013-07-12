package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
public class Split5gramBolt {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}
}
*/

//IBasicBolt ensure message guarantee by auto ack-ing GSWS pg 42

public class  Split5gramBolt extends BaseBasicBolt {
//public class  Split5gramBolt implements IRichBolt {//need to impl several other methods
//private OutputCollector collector;//IRichBolt style
	
public void cleanup() {}
	/**
	 * The bolt will receive the line from the
	 * words file and process it to Normalize this line
	 * 
	 * The normalize will be put the words in lower case
	 * and split the line to get all words in this 
	 */
	
	//called once every time tuple received; nextTuple() and execute() can emit 0 ,1 or many tuples.
	public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getString(0);
        String [] terms = sentence.split("\\s+"); 
        
        for(int i = 0;i<terms.length; i++){
        	if(terms[i].length()==0){
        		continue;
        		
        	}
        	if(terms[i]==null){
        		continue;
        	}
        }//end for
        
        //bigram inverted index the mapper equivalent
        int i = 0;
        String tf = terms[terms.length-1];//used for sorting by popularity
        String bigram = terms[i] + " "+ terms[i+1];
        String txt = tf + " " + terms[2] +" "+ terms[3]+" "+terms[4]; // tf w1 w2 w3; to save mem space; sort by tf as the posting list.
        //MR style emit (bigram, sentence); need equivalent style in storm. eg sort by key via fields grouping.
        		
        //collector.emit(bigram, new Values(txt));//emit(streamID, <List> tuple)//did not emit properly
        //collector.emit(new Values(bigram));
        
      //emit 2field tuple <key,val> style mod@declarer
        collector.emit(new Values(bigram,txt));
        
        //shin: maybe this is good place to pump into redis the <key,val> = <bigram,txt>//downside is cant sort by tf here.       
/*  //wordcount      
        String[] words = sentence.split(" ");
        for(String word : words){
            word = word.trim();
            if(!word.isEmpty()){
                word = word.toLowerCase();
                collector.emit(new Values(word));
                
                //IRichBolt style
                //List a = new ArrayList();
                //a.add(input);
                //collector.emit(a,new Values(word));
            }//end if
        }//end for
  */      
        
        //IRichBolt style ack the tuple; exp frm book
        //collector.ack(input);// .ack() not found
	}//end execute
	

	/**
	 * The bolt will only emit the field "word" 
	 */
	//declare the putput param of this bolt
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("word"));//this bolt will emit 1 field tuple named "word"
		declarer.declare(new Fields("bigram","txt"));//emit 2field tuple <key,val> style
	}//end declare out fields

//need <key,val> type of tuple emitted	
	
/*	
//use with IRichBolt
	public void prepare(Map stormConf, TopologyContext Context, OutputCollector collector){
		this.collector = collector;
		
	}
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
*/ 	
}//end class
