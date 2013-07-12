package storm.myStuff;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


//import spouts and bolts
import storm.starter.spout.redisQreadSpout;
import storm.starter.bolt.QtoRTiiBolt;

public class myreadQTopo {
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("redisQreadSpout",new redisQreadSpout());
		builder.setBolt("QtoRTiiBolt", new QtoRTiiBolt())//.fieldsGrouping("redisQreadSpout", new Fields("items"));//mod@declarer
		.shuffleGrouping("redisQreadSpout");//use random shuffle
        //Configuration
		Config conf = new Config();
		conf.setDebug(false);
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();//use local mode
		cluster.submitTopology("readQtopo", conf, builder.createTopology());
		Thread.sleep(60000);//run for 5min *60 = 300000ms only. run forever remove this line
		cluster.shutdown();
		
	}//end main
}//end topo
