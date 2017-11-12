package ass4;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class ReverseGraph {
	static class IntPair implements WritableComparable<IntPair> {
		private int first,second;
		
		public IntPair(){
			
		}
		
		public IntPair(int first,int second){
			set(first,second);
		}
		
		public void set(int left, int right){
			first = left;
			second = right;
		}
		
		public int getFirst(){
			return first;
		}
		
		public int getSecond(){
			return second;
		}
		
		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			first = in.readInt();
			second = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			
			out.writeInt(first);
			out.writeInt(second);
			
		}
		private int compare(int a, int b){
			if (a == b){
				return 0;
			} else if (a > b){
				return 1;
			} else {
				return -1;
			}
		}
		

		@Override
		public int compareTo(IntPair o) {
			// TODO Auto-generated method stub
			int cmp = compare(first,o.getFirst());
			if(cmp!=0){
				return cmp;
			}
			return compare(second,o.getSecond());
		}

				
	}

	
	public static class reverseGraphMapper extends Mapper<Object,Text,IntPair,NullWritable>{
		private IntPair pair = new IntPair();
		private final static NullWritable nullOutput = NullWritable.get();
		
		public void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			
			while(itr.hasMoreTokens()){
				String element = itr.nextToken();
				if(!element.contains("#")){
					String[] node = element.split("\t");
					
					pair.set(Integer.parseInt(node[1]),Integer.parseInt(node[0]));
					
					context.write(pair,nullOutput);
					
					
				}
			}
			
			
			
		}
	}
	
	public static class reverseGraphPartitioner extends Partitioner<IntPair,NullWritable>{
		
		@Override
		public int getPartition(IntPair key, NullWritable value, int numPartitions) {
			// TODO Auto-generated method stub
			return key.getFirst()  % numPartitions;
		}

		
	}
	
	public static class reverseGraphReducer extends Reducer<IntPair,NullWritable,Text,Text>{
		private static String outVertex ="";
		private static int curSource = -1;
	
		
		
		public void reduce(IntPair key, Iterable<NullWritable> values,Context context) 
				throws IOException, InterruptedException{
			
			int source = key.getFirst();
			
			if (source == curSource){
				outVertex += key.getSecond()+ ",";
			} else {
				if (curSource != -1) {
					if(curSource%3 ==source%3){
						context.write(new Text(curSource+"\t"),new Text(outVertex.substring(0,outVertex.length()-1)));
					}
				} 
				curSource = source;
				outVertex = key.getSecond() +",";
		
			}
			
			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			context.write(new Text(curSource+"\t"),new Text(outVertex.substring(0,outVertex.length()-1)));
			
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"reverse graph");
		job.setJarByClass(ReverseGraph.class);
		job.setMapperClass(reverseGraphMapper.class);
		job.setReducerClass(reverseGraphReducer.class);
		job.setPartitionerClass(reverseGraphPartitioner.class);
		
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(3);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
