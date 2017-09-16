package stubs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * Author: Akhilesh A Borgaonkar
 * UB ID: 0997553
 * Course: CPSC-651-DL-Big Data Systems & Analysis-2016
 * Title: Analyzing trends in Vehicle Collisions
 *
 * The following is the code for partitioner class:
 */

public class TVCPartitioner <K2, V2> extends Partitioner<Text, IntWritable> {
	
public int getPartition(Text key,IntWritable value, int numReduceTasks) {
//read the key and parse the year field
		String line = key.toString();
		int year = Integer.parseInt(line.split(",(?=(?:(?:[^\"]*+\"){2})*+[^\"]*+$)",-1)[0]);
		int partitn = year%10;
		return  partitn% numReduceTasks;
	}
}