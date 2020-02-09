/**
 * @file
 * @author  Toufiqur Rahman Chowdhury
 * @email <tchowdhu@my.bridgeport.edu>
 * @id 946264
 * 
 * @author  Arun Sai Prakash Arumugam 
 * @email <aarumuga@my.bridgeport.edu>
 * @id 976330
 * 
 * @version 2.0
 *
 * @section LICENSE
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details at
 * http://www.gnu.org/copyleft/gpl.html
 *
 * @section DESCRIPTION
 *
 * This is the driver class to run CDR Analytics program named as CDRParser
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.util.GenericOptionsParser;


public class CDRParser {
	
	/**
	 * 
	 * Main function for CDR Analytics program
	 *
	 * @param  This function contains the configuration of the JOB task 
	 * @return void
	 * 
	 */
	
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: CDRParser <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("CDRParser");
 
        job.setJarByClass(CDRParser.class);
        job.setMapperClass(CDRParserMapper.class);
        //job.setCombinerClass(CDRParserCombiner.class);
        //job.setReducerClass(CDRParserReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        //job.setOutputKeyClass(IntWritable.class);
        //job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
