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
 * This is the Mapper class for parsing the input file named as CDRParserMapper
 * input <LongWritable, Text>
 * output <Text, Text>
 * output format <"SUBNO" "DURATION|B SUBNO|STARTTIME">
 * 
 */

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The mapper reads one line at the time, splits it into an array of single words and emits every
 * word to the reducers with the value of 1.
 */

public class CDRParserMapper extends Mapper<LongWritable, Text, Text, Text> {

//        private final static IntWritable one = new IntWritable(1);
//        private Text word = new Text();

		/**
		 *
		 * @param  This is map function for CDR Analytics program and runs to parse the CDR data
		 * @param  <Text, IntWritable> 
		 * @return <Text, IntWritable>
		 * 
		 */

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //while (itr.hasMoreTokens()) {
                //word.set(itr.nextToken().trim());
                //context.write(word, one);
        	
        	  String callValue;        	  
//        	  String[] tokens;
        	
        	  String line = value.toString();
        	    
        	  String word1 = line.substring(25,38).trim();	    // Subscriber No. 
        	  String word3 = line.substring(124,140).trim();	// Destination
        	  String word2 = line.substring(191,195).trim();    // Duration
          	  String word4 = line.substring(179,189).trim();	// Time-stamp
       
        	  //if (word1.length() > 0 & !word2.isEmpty()) {
        	  
        	  if (word1.length() > 0) {        		  	
        		  if(word2.isEmpty())
        			  word2 = "0"; 
        		 
        	      
        		  callValue = word2 + "|"+ word3 + "|" + word4; 
        		  
        	      //context.write(new Text(word1), new IntWritable(Integer.parseInt(word2)));        		  
        		  //tokens = val.toString().split("|");
        		  
        		  context.write(new Text(word1), new Text(callValue));
        	      
        	    }
        	  
        	  	//String[] tokens = callValue.toString().split("|");
        	
            }
   }

  
