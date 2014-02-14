package storm.sohu.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransportException;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;




public class StormMonitorTester {

	public static Logger LOG = Logger.getLogger(StormMonitorTester.class);    

	
	public static void main(String... args){
		
		//default nimbus host
		String host = "10.16.1.226";    
		//default nimbus port
	    int port = 6627;                    
		
		TSocket tsocket = new TSocket(host, port);
		TFramedTransport tTransport = new TFramedTransport(tsocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		 Nimbus.Client  client = new Nimbus.Client(tBinaryProtocol);
		try {
			tTransport.open();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
		
		try {
			ClusterSummary clusterSummary = client.getClusterInfo();
			LOG.info("=================ClusterSummary=========================");
			LOG.info(clusterSummary.get_nimbus_uptime_secs());
			LOG.info(clusterSummary.get_supervisors_size());
			LOG.info(clusterSummary.get_topologies_size());
			List<SupervisorSummary>  supervisorSummaryLt = clusterSummary.get_supervisors();
			for(SupervisorSummary ss : supervisorSummaryLt ){
				LOG.info("=================SupervisorSummary=========================");
				LOG.info(ss.get_host());
				LOG.info(ss.get_num_used_workers());
				LOG.info(ss.get_num_workers());
				LOG.info(ss.get_supervisor_id());
				LOG.info(ss.get_uptime_secs());
			}
			
			List<TopologySummary> topologySummaryLt = clusterSummary.get_topologies();
			for(TopologySummary ts : topologySummaryLt ){
				LOG.info("=================TopologySummary=========================");
				LOG.info(ts.get_id());
				LOG.info(ts.get_name());
				LOG.info(ts.get_num_executors());
				LOG.info(ts.get_num_tasks());
				LOG.info(ts.get_num_workers());
				LOG.info(ts.get_status());
				LOG.info(ts.get_uptime_secs());
			}
			
			//key : host_component
			//value : emit or transit value
			HashMap<String, ArrayList<String>> rs = new HashMap<String, ArrayList<String>>();
		
			TopologyInfo topologyInfo = client.getTopologyInfo("word123-12-1390474015");
			List<ExecutorSummary> executorSummaries = topologyInfo.get_executors();
			for(ExecutorSummary executorSummary : executorSummaries) {
				LOG.info("=================ExecutorSummary[KafkaTopology5-10-1390472210]=========================");
				LOG.info(executorSummary.get_component_id());
				LOG.info(executorSummary.get_host());
				LOG.info(executorSummary.get_port());
				LOG.info(executorSummary.get_uptime_secs());
		
			
				
				ExecutorInfo executorInfo = executorSummary.get_executor_info();
				LOG.info(executorInfo.get_task_start());
				LOG.info(executorInfo.get_task_end());
				
				ExecutorStats executorStats = executorSummary.get_stats();
				LOG.info(executorStats == null);
				
				String host_componet = String.format("%s\t%s",  executorSummary.get_host(),executorSummary.get_component_id());	
				LOG.info("host_componet="+host_componet);
		
				
//				ObjectMapper mapper = new ObjectMapper();
//				try {
//					//transit
//					Map<String,Map<String,Long>> transData = executorStats.get_transferred();
//					mapper.writeValue(System.out, transData);
//				    LOG.info(mapper.writeValueAsString(transData));			           
//					//emitt	
//					Map<String,Map<String,Long>> emittData = executorStats.get_emitted();
//					//mapper.writeValue(System.out, emittData);
//					LOG.info(mapper.writeValueAsString(emittData));		
//					
//				} catch (JsonGenerationException e1) {
//					e1.printStackTrace();
//				} catch (JsonMappingException e1) {
//					e1.printStackTrace();	
//				} catch (IOException e1) {
//					e1.printStackTrace();
//						
//				}		
				
				   
				// 处理 transit 类型的数据 
				if (executorStats.get_transferred().get(":all-time").size() == 0){
					
					if (!rs.containsKey(host_componet)){
						ArrayList<String> tmpArray = new ArrayList<String>();
						tmpArray.add("transit\t" + 0);
						rs.put(host_componet, tmpArray);
					}
					else {
						rs.get(host_componet).add("transit\t" + 0);
					}
				} else {
					
					String tmp = executorStats.get_transferred().get(":all-time").get("default") + "";
					
					if (!rs.containsKey(host_componet)){
						ArrayList<String> tmpArray = new ArrayList<String>();
						tmpArray.add("transit\t" + (tmp.equals("null") ? "0" : tmp));
						rs.put(host_componet, tmpArray);
					} else {
						
						rs.get(host_componet).add("transit\t" + (tmp.equals("null") ? "0" : tmp));
					}
					
				}
				
				//处理 emitted 类型的数据
				// {":all-time":{"default":3481240},"600":{"default":36440},"10800":{"default":653320},"86400":{"default":3481240}}
				if (executorStats.get_emitted().get(":all-time").size() == 0){
					if (!rs.containsKey(host_componet)){
						ArrayList<String> tmpArray = new ArrayList<String>();
						tmpArray.add("emmitted\t" + 0);
						rs.put(host_componet, tmpArray);
					}
					else {
						rs.get(host_componet).add("emitted\t" + 0);
					}
					
				} else {
					String tmp = executorStats.get_emitted().get(":all-time").get("default") + "";
					
					if (!rs.containsKey(host_componet)){
						ArrayList<String> tmpArray = new ArrayList<String>();
						tmpArray.add("emitted\t" + (tmp.equals("null") ? "0" : tmp));
						rs.put(host_componet, tmpArray);
					} else {
						rs.get(host_componet).add("emitted\t" + (tmp.equals("null") ? "0" : tmp));
					}
					
				}
				
			}//end for
			
			LOG.info("=================Static=========================");
			for(Map.Entry<String, ArrayList<String>> e : rs.entrySet()){
				LOG.info("key:"+e.getKey());
				for(String s: e.getValue()){
					LOG.info("value:"+s);
				}
			}//end for
			
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotAliveException e) {
			e.printStackTrace();
		}finally{
			tTransport.close();
			tsocket.close();
		}
		
	}
	

}

	