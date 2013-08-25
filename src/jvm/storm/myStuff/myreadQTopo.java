package storm.myStuff;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


//import spouts and bolts
import storm.starter.spout.redisQreadSpout;
import storm.starter.bolt.QtoRTiiBolt;

public class myreadQTopo {
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub

        //Configuration
		Config conf = new Config();
		conf.setNumWorkers(2);//020813:set 2 worker process; aws storm deploy no worker assigned//140813:set 4worker for 4 sup instances
		conf.setDebug(false);
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("redisQreadSpout",new redisQreadSpout())
		.setNumTasks(2);//,2 to put 2 instances
		builder.setBolt("QtoRTiiBolt", new QtoRTiiBolt())
		.setNumTasks(2)
		//,2 to put 2 instances//.fieldsGrouping("redisQreadSpout", new Fields("items"));//mod@declarer
		.shuffleGrouping("redisQreadSpout");//use random shuffle
		

		
		
//		//local mode + redis + redis works
//		LocalCluster cluster = new LocalCluster();//use local mode
//		cluster.submitTopology("readQtopo", conf, builder.createTopology());
//		Thread.sleep(60000);//run for 5min *60 = 300000ms only. run forever remove this line
//		cluster.shutdown();
		
				
		//using remote mode
        StormSubmitter.submitTopology("myReadQtopo", conf, builder.createTopology());
		
		
	}//end main
}//end topo
