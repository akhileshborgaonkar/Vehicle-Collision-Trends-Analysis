package stubs;

import java.io.IOException; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 *
 *  Following is the mapper class:
 */

public class TVCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * Break the fields from line which is received as a Text object,
     * and convert it to a String object.
     */
      
      String line = value.toString();
      String input[]=line.split(",(?=(?:(?:[^\"]*+\"){2})*+[^\"]*+$)",-1);
      String year = input[0].split("-")[0];
      String location = input[2];
      String reason = input[11] ;//+ "," + input[12];
      String combi = year + "," + location + "," + reason;
  
        if (combi.length() > 0){
        /*
         * Call the write method on the Context object to emit a key
         * and a value from the map method.
         */
        context.write(new Text(combi), new IntWritable(1));
      }
    }
  }
