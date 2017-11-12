package ass4;

import java.io.IOException;
import java.net.URL;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import ass4.ReverseGraph.IntPair;
import ass4.ReverseGraph.reverseGraphMapper;
import ass4.ReverseGraph.reverseGraphPartitioner;
import ass4.ReverseGraph.reverseGraphReducer;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;


public class SingleSourceSP {
	
	public static final double MAX_DISTANCE = Double.MAX_VALUE;
	public static String OUT = "";
	public static String IN = "";
	public static long source = -1;
	
	public static boolean graph_is_modified = true;
	
	
	public static class AdjacentListMapper extends Mapper<Object, Text,LongTuple,NullWritable >{
		private final static NullWritable nullOutput = NullWritable.get();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			
			while(itr.hasMoreTokens()){
				String element = itr.nextToken();
				
					String[] node = element.split(" ");
					long fromNode = Long.parseLong(node[1]);
					long toNode = Long.parseLong(node[2]);
					double distance = Double.parseDouble(node[3]);
					
					context.write(new LongTuple(fromNode, toNode, distance), nullOutput);
					
				}
		}
	}
	
	
	public static class AdjacentListReducer extends Reducer<LongTuple,NullWritable,Text,Text>{
		private static String adjacentList = "";
		private static long lastSource = -1;
		
		public void reduce(LongTuple key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException{
			long fromNode = key.getFromNode();
			
			if (fromNode == lastSource){
				adjacentList += key.getToNode() + ":" + key.getDistance()+",";
				
			} else {

				if (lastSource != -1 && lastSource == source){
		
					context.write(new Text(lastSource + "\t" +0),new Text(adjacentList.substring(0,adjacentList.length()-1)));
				} else if (lastSource != -1 && lastSource != source) {
			
					context.write(new Text(lastSource + "\t" + MAX_DISTANCE),new Text(adjacentList.substring(0,adjacentList.length()-1)));
				}
				lastSource = fromNode;
				adjacentList = key.getToNode()+":" + key.getDistance() +",";
			}
			
			
		}
		
		public void cleanup (Context context) throws IOException, InterruptedException{
			if (lastSource != -1 && lastSource == source){
				
				context.write(new Text(lastSource + "\t" +0),new Text(adjacentList.substring(0,adjacentList.length()-1)));
			} else if (lastSource != -1 && lastSource != source) {
		
				context.write(new Text(lastSource + "\t" + MAX_DISTANCE),new Text(adjacentList.substring(0,adjacentList.length()-1)));
			}
		}
	}
	
	
	public static class SPMapper extends Mapper<Object,Text,LongWritable, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			// adjacentlist format  0(fromNode)	0(distance)		1:5.394655, 2:10.284244, 5:40.444225
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			//System.out.println("In SPMapper");
			while(itr.hasMoreTokens()){
				String element = itr.nextToken();
				//System.out.println("element = " + element);
				String[] node_info = element.trim().split("\t");//[0,0,(1:4,3:21.2)]
				
				long fromNode = Long.parseLong(node_info[0]);
				//System.out.println("fromNode = " + fromNode);
				double distance = Double.parseDouble(node_info[1]);
				//System.out.println("distance = " + distance);
				
				String adjacencyList = node_info[2].trim();
				//System.out.println(adjacencyList);
				if (adjacencyList.equals("null")){
					context.write(new LongWritable(fromNode), new Text("node#" + fromNode+'\t'+distance+'\t'+"null"));
					return;
				}
				String[] outgoings = node_info[2].trim().split(",");
			
				context.write(new LongWritable(fromNode), new Text("node#" + value.toString()));
					
				for(String adjacentNode_Distance : outgoings){
						
					long toNode = Long.parseLong(adjacentNode_Distance.split(":")[0]);
					double weight = Double.parseDouble(adjacentNode_Distance.split(":")[1]);
					if (distance != MAX_DISTANCE){
						//System.out.println("pasing graph structure");
						context.write(new LongWritable(toNode), new Text("distance#" + String.valueOf(distance + weight)));
					} else {
						//System.out.println("pasing distance");
						//context.write(new LongWritable(toNode), new Text("distance#" + String.valueOf(distance)));
					}
				
			
				}
			}
		}
		
		
	}
	
	
	public static class SPReducer extends Reducer<LongWritable,Text,LongWritable,Text>{
		// key = 1   value = node#1 1.7888 2:8
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String node =null;
			String adjacencyList = null;
			double minDistance = Double.MAX_VALUE;
			//System.out.println("In SPReducer");
			String[] valArray = null;

			for(Text val : values) {
				//System.out.println("val = " + val);
				valArray = val.toString().split("#");
				
				if(valArray[0].compareTo("node") == 0){//node# 1	1.7976931348623157E308		2:7.72414,4:8.802094
					node = valArray[1];   // 1	1.7976931348623157E308		2:7.72414,4:8.802094
				} else if (valArray[0].compareTo("distance") == 0){//distance,5
					if (Double.parseDouble(valArray[1].trim()) < minDistance){
						minDistance = Double.parseDouble(valArray[1].trim());
					}
				}
			}
			
			if(node == null){	
				context.write(key,new Text(String.valueOf(minDistance)+'\t' + "null"));
				return;
			}
			
			if(node.equals("null")){
				context.write(key,new Text(valArray[1].split("\t")[1]+'\t' + "null"));
				return;
			}
			
			adjacencyList = node.split("\t")[2];
			double curDistance = Double.parseDouble(node.split("\t")[1].trim());
			
			if (minDistance < curDistance){
				String curNode = node.split("\t")[0].trim();
				double new_distance = minDistance;
	
				Text new_adjList = new Text(new_distance+"\t"+adjacencyList);
				context.write(key,new_adjList);
				graph_is_modified = true;
				//System.out.println("modifying the graph structure");
			} else {
				String curNode = node.split("\t")[0].trim();
			
				context.write(key,new Text(curDistance+"\t"+adjacencyList));
				//System.out.println("no modification");
			}
			
			
		}
	}
	
	public static class finalMapper extends Mapper<Object, Text, LongWritable, Text> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			while(itr.hasMoreTokens()){
				String element = itr.nextToken();
				String node = element.split("\t")[0];
				String distance = element.split("\t")[1];
				context.write(new LongWritable(Long.parseLong(node)),new Text(distance));
			}
		}
	}
	
	public static class finalReducer extends Reducer<LongWritable,Text,LongWritable,Text> {
		public void reduce(LongWritable key,Iterable<Text> values, Context context) throws IOException,InterruptedException{
			for(Text val : values){
				context.write(key,new Text(val));
			}
		}
	}
	
	public static void deleteFolder(Configuration conf,String folderPath) throws IOException {
		
		conf.addResource(new Path("/home/comp9313/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/home/comp9313/hadoop/etc/hadoop/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		
		if(fs.exists(path)){
			fs.delete(path,true);
		}
	}
	
	
	public static void main(String[] args) throws Exception{
		IN = args[0];
		OUT = args[1];
		source = Long.parseLong(args[2]);
		
		String input = IN;
		String output = OUT + System.nanoTime();
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"reverse graph");
		job.setJarByClass(SingleSourceSP.class);
		job.setMapperClass(AdjacentListMapper.class);
		job.setReducerClass(AdjacentListReducer.class);
		
		
		job.setMapOutputKeyClass(LongTuple.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output));
		job.waitForCompletion(true);
		int i = 0;
		int j = 0;
		input = output;
		
		output = OUT + System.nanoTime();
		while(graph_is_modified){
			
			i++;
			j++;
			
			//System.out.println("Iteration = " + i);
			graph_is_modified = false;
			
			
			
			conf = new Configuration();
			job = Job.getInstance(conf,"shortest path");
			job.setJarByClass(SingleSourceSP.class);
			job.setMapperClass(SPMapper.class);
			job.setReducerClass(SPReducer.class);
			
			
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			
			job.waitForCompletion(true);
			
			deleteFolder(conf,input);
			
			
			input = output;
			
			output = OUT + System.nanoTime();
			
		}
		
		
		conf = new Configuration();
		job = Job.getInstance(conf,"reverse graph");
		job.setJarByClass(SingleSourceSP.class);
		job.setMapperClass(finalMapper.class);
		job.setReducerClass(finalReducer.class);
		
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(OUT));
		
		
		job.waitForCompletion(true);
		
		deleteFolder(conf,input);
		
		
	}
	
	
	
	
	static class LongTuple implements WritableComparable<LongTuple> {
		private long fromNode;
		private long toNode;
		private double distance;
		
		public LongTuple(){
			
		}
		
		public LongTuple(long fromNode, long toNode, double distance){
			set(fromNode,toNode, distance);
		}
		
		public void set(long left, long right, double d){
			fromNode = left;
			toNode = right;
			distance = d;
		}
		
		public long getFromNode(){
			return fromNode;
		}
		
		public long getToNode(){
			return toNode;
		}
		
		public double getDistance(){
			return distance;
		}
		
		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			fromNode = in.readLong();
			toNode = in.readLong();
			distance = in.readDouble();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			
			out.writeLong(fromNode);
			out.writeLong(toNode);
			out.writeDouble(distance);
			
		}
		private int compare(long a, long b){
			if (a == b){
				return 0;
			} else if (a > b){
				return 1;
			} else {
				return -1;
			}
		}
		

		public int compareTo(LongTuple o) {
			// TODO Auto-generated method stub
			int cmp = compare(fromNode,o.getFromNode());
			if(cmp!=0){
				return cmp;
			}
			return compare(toNode,o.getToNode());
		}

				
	}
	
	
}
