/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import CustomWritables.DTJrPairElement;
import DataTypes.PointSP;
import DataTypes.PointST;

/**
 * This class treats a line in the input as a key/value pair separated by a 
 * separator character. The separator can be specified in config file 
 * under the attribute name key.value.separator.in.input.line. The default
 * separator is the tab character ('\t').
 */
public class KeyValueCustomRecordReader extends RecordReader<DTJrPairElement, Text> {
  
  private final LineRecordReader lineRecordReader;

  private byte separator = (byte) '\t';

  private Text innerValue;

  private DTJrPairElement key;
  
  private Text value;
  
  public Class<?> getKeyClass() { return Text.class; }
  
  public KeyValueCustomRecordReader(Configuration conf)
    throws IOException {
    
    lineRecordReader = new LineRecordReader();
    String sepStr = conf.get("key.value.separator.in.input.line", "\t");
    this.separator = (byte) sepStr.charAt(0);
  }

  public void initialize(InputSplit genericSplit,
      TaskAttemptContext context) throws IOException {
    lineRecordReader.initialize(genericSplit, context);
  }
  
  public static int findSeparator(byte[] utf, int start, int length, 
      byte sep) {
    for (int i = start; i < (start + length); i++) {
      if (utf[i] == sep) {
        return i;
      }
    }
    return -1;
  }

  public static void setKeyValue(DTJrPairElement key, Text value, byte[] line,
      int lineLen, int pos) {
	  
	Text temp_key = new Text();
	int n_of_args = 0;
    if (pos == -1) {
      temp_key.set(line, 0, lineLen);
      String key_line = temp_key.toString();

      StringTokenizer linetokenizer = new StringTokenizer(key_line, ",");
      n_of_args = linetokenizer.countTokens();
      
      if (n_of_args == 10){
          
    	  while (linetokenizer.hasMoreTokens()) {

    	      key.setr_obj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.setr_traj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.setr_point(new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()))));
    	      key.sets_obj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.sets_traj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.sets_point(new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()))));
          }

      } else if (n_of_args == 12){
    	  
    	  while (linetokenizer.hasMoreTokens()) {

    	      key.setr_obj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.setr_traj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.setr_point(new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()))));
    	      key.sets_obj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.sets_traj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.sets_point(new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()))));
          }
 	  
      }

      value.set("");

    } else {
      int keyLen = pos;
      byte[] keyBytes = new byte[keyLen];
      System.arraycopy(line, 0, keyBytes, 0, keyLen);
      int valLen = lineLen - keyLen - 1;
      byte[] valBytes = new byte[valLen];
      System.arraycopy(line, pos + 1, valBytes, 0, valLen);
      temp_key.set(keyBytes);
      String key_line = temp_key.toString();
      StringTokenizer linetokenizer = new StringTokenizer(key_line, ",");
      n_of_args = linetokenizer.countTokens();

      if (n_of_args == 10){
          
    	  while (linetokenizer.hasMoreTokens()) {

    	      key.setr_obj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.setr_traj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.setr_point(new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()))));
    	      key.sets_obj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.sets_traj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.sets_point(new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()))));
          }

      } else if (n_of_args == 12){
    	  
    	  while (linetokenizer.hasMoreTokens()) {

    	      key.setr_obj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.setr_traj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.setr_point(new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()))));
    	      key.sets_obj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.sets_traj_id(Integer.parseInt(linetokenizer.nextToken()));
    	      key.sets_point(new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()))));
          }
 	  
      }
      value.set(valBytes);
      
    }
  }
  /** Read key/value pair in a line. */
  public synchronized boolean nextKeyValue()
    throws IOException {
    byte[] line = null;
    int lineLen = -1;
    if (lineRecordReader.nextKeyValue()) {
      innerValue = lineRecordReader.getCurrentValue();
      line = innerValue.getBytes();
      lineLen = innerValue.getLength();
    } else {
      return false;
    }
    if (line == null)
      return false;
    if (key == null) {
      key = new DTJrPairElement();
    }
    if (value == null) {
      value = new Text();
    }
    int pos = findSeparator(line, 0, lineLen, this.separator);
    setKeyValue(key, value, line, lineLen, pos);

    return true;
  }
  
  public DTJrPairElement getCurrentKey() {
    return key;
  }

  public Text getCurrentValue() {
    return value;
  }

  public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }
  
  public synchronized void close() throws IOException { 
    lineRecordReader.close();
  }
}
