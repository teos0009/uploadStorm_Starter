package storm.myStuff;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


//import spouts and bolts
import storm.starter.spout.consumeDataGovSpout;
import storm.starter.bolt.clicksRtiiBolt;


public class myClickConsumeTopo {
	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
		// TODO Auto-generated method stub

        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("consumeDataGovSpout",new consumeDataGovSpout());
		builder.setBolt("clicksRtiiBolt", new clicksRtiiBolt()) //set 2 instances of bolt with ",2"
		    //  .shuffleGrouping("consumeDataGovSpout");//sending tuples to bolt at random
		.fieldsGrouping("consumeDataGovSpout", new Fields("country"));//mod@declarer
		//fields grouping with "country" ensure tuple will be send to the same clicksRtiiBolt
		
		
        //Configuration
		Config conf = new Config();
		conf.setNumWorkers(2);
		conf.setDebug(false);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        
		//Topology run
	    //local mode
//		LocalCluster cluster = new LocalCluster();//use local mode
//		cluster.submitTopology("myClickTopo", conf, builder.createTopology());
//		Thread.sleep(60000);//run for x milsec
//		cluster.shutdown();
		
		//using remote mode
        StormSubmitter.submitTopology("myClickTopo", conf, builder.createTopology());
		
	}//end main
}//end topo
