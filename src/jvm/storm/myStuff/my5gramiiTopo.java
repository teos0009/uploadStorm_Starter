package storm.myStuff;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


//import spouts and bolts
import storm.starter.spout.Read5gramSpout;
import storm.starter.bolt.Split5gramBolt;
import storm.starter.bolt.Store5gramiiBolt;

public class my5gramiiTopo {
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("read5gram",new Read5gramSpout());
		builder.setBolt("split5gram", new Split5gramBolt())
			.shuffleGrouping("read5gram");//storm send each msg to an instance of bolt in random dist
		  //.fieldsGrouping("read5gram", new Fields("line"));//ideal case of same word send to same word counter
		
		//grouping can leverage as MR's shuffle and sort framework???
		
		builder.setBolt("store5gram", new Store5gramiiBolt(),2) //set 2 instances of bolt
		    //  .shuffleGrouping("split5gram");//sending tuples to bolt at random
			//.fieldsGrouping("split5gram", new Fields("word"));
		.fieldsGrouping("split5gram", new Fields("bigram"));//mod@declarer
		//fields groupingwith "bigram" ensure tuple will be send to the same store5gram
		
		String path = "/home/user/Downloads/smallset5gm";
        //Configuration
		Config conf = new Config();
		//conf.put("wordsFile", args[0]);//set path to file here
		conf.put("wordsFile", path);//wordsFile is var used to pass path to spout
		conf.setDebug(false);
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();//use local mode
		cluster.submitTopology("II5gram", conf, builder.createTopology());
		Thread.sleep(10000);//run for default 1000milli sec
		cluster.shutdown();
		
	}//end main
}//end topo
