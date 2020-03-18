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

package org.apache.hadoop.mapreduce.lib.partition;

import java.io.IOException;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import DataTypes.BoxSP;
import DataTypes.BoxST;
import DataTypes.OctTreeNode;
import DataTypes.PointSP;
import DataTypes.PointST;

/**
 * Utility for collecting samples and writing a partition file for
 * {@link TotalOrderPartitioner}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CreateOctTreePartitionFile<K,V> extends Configured implements Tool  {

  private static final Log LOG = LogFactory.getLog(CreateOctTreePartitionFile.class);

  static int printUsage() {
    System.out.println("sampler -r <reduces>\n" +
      "      [-inFormat <input format class>]\n" +
      "      [-keyClass <map input & output key class>]\n" +
      "      [-splitRandom <double pcnt> <numSamples> <maxsplits> | " +
      "             // Sample from random splits at random (general)\n" +
      "       -splitSample <numSamples> <maxsplits> | " +
      "             // Sample from first records in splits (random data)\n"+
      "       -splitInterval <double pcnt> <maxsplits>]" +
      "             // Sample from splits at intervals (sorted data)");
    System.out.println("Default sampler: -splitRandom 0.1 10000 10");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  public CreateOctTreePartitionFile(Configuration conf) {
    setConf(conf);
  }

  /**
   * Interface to sample using an 
   * {@link org.apache.hadoop.mapreduce.InputFormat}.
   */
  public interface Sampler<K,V> {
    /**
     * For a given job, collect and return a subset of the keys from the
     * input data.
     */
    K[] getSample(InputFormat<K,V> inf, Job job) 
    throws IOException, InterruptedException;
  }

  /**
   * Samples the first n records from s splits.
   * Inexpensive way to sample random data.
   */
  public static class SplitSampler<K,V> implements Sampler<K,V> {

    protected final int numSamples;
    protected final int maxSplitsSampled;

    /**
     * Create a SplitSampler sampling <em>all</em> splits.
     * Takes the first numSamples / numSplits records from each split.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     */
    public SplitSampler(int numSamples) {
      this(numSamples, Integer.MAX_VALUE);
    }

    /**
     * Create a new SplitSampler.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     * @param maxSplitsSampled The maximum number of splits to examine.
     */
    public SplitSampler(int numSamples, int maxSplitsSampled) {
      this.numSamples = numSamples;
      this.maxSplitsSampled = maxSplitsSampled;
    }

    /**
     * From each split sampled, take the first numSamples / numSplits records.
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, Job job) 
        throws IOException, InterruptedException {
      List<InputSplit> splits = inf.getSplits(job);
      ArrayList<K> samples = new ArrayList<K>(numSamples);
      int splitsToSample = Math.min(maxSplitsSampled, splits.size());
      int samplesPerSplit = numSamples / splitsToSample;
      long records = 0;
      for (int i = 0; i < splitsToSample; ++i) {
        TaskAttemptContext samplingContext = new TaskAttemptContextImpl(
            job.getConfiguration(), new TaskAttemptID());
        RecordReader<K,V> reader = inf.createRecordReader(
            splits.get(i), samplingContext);
        reader.initialize(splits.get(i), samplingContext);
        while (reader.nextKeyValue()) {
          samples.add(ReflectionUtils.copy(job.getConfiguration(),
                                           reader.getCurrentKey(), null));
          ++records;
          if ((i+1) * samplesPerSplit <= records) {
            break;
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * Sample from random points in the input.
   * General-purpose sampler. Takes numSamples / maxSplitsSampled inputs from
   * each split.
   */
  public static class RandomSampler<K,V> implements Sampler<K,V> {
    protected double freq;
    protected final int numSamples;
    protected final int maxSplitsSampled;

    /**
     * Create a new RandomSampler sampling <em>all</em> splits.
     * This will read every split at the client, which is very expensive.
     * @param freq Probability with which a key will be chosen.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     */
    public RandomSampler(double freq, int numSamples) {
      this(freq, numSamples, Integer.MAX_VALUE);
    }

    /**
     * Create a new RandomSampler.
     * @param freq Probability with which a key will be chosen.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     * @param maxSplitsSampled The maximum number of splits to examine.
     */
    public RandomSampler(double freq, int numSamples, int maxSplitsSampled) {
      this.freq = freq;
      this.numSamples = numSamples;
      this.maxSplitsSampled = maxSplitsSampled;
    }

    /**
     * Randomize the split order, then take the specified number of keys from
     * each split sampled, where each key is selected with the specified
     * probability and possibly replaced by a subsequently selected key when
     * the quota of keys from that split is satisfied.
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, Job job) 
        throws IOException, InterruptedException {
      List<InputSplit> splits = inf.getSplits(job);
      ArrayList<V> samples = new ArrayList<V>(numSamples);
      int splitsToSample = Math.min(maxSplitsSampled, splits.size());

      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      LOG.debug("seed: " + seed);
      // shuffle splits
      for (int i = 0; i < splits.size(); ++i) {
        InputSplit tmp = splits.get(i);
        int j = r.nextInt(splits.size());
        splits.set(i, splits.get(j));
        splits.set(j, tmp);
      }
      // our target rate is in terms of the maximum number of sample splits,
      // but we accept the possibility of sampling additional splits to hit
      // the target sample keyset
      for (int i = 0; i < splitsToSample ||
                     (i < splits.size() && samples.size() < numSamples); ++i) {
        TaskAttemptContext samplingContext = new TaskAttemptContextImpl(
            job.getConfiguration(), new TaskAttemptID());
        RecordReader<K,V> reader = inf.createRecordReader(
            splits.get(i), samplingContext);
        reader.initialize(splits.get(i), samplingContext);
        while (reader.nextKeyValue()) {
          if (r.nextDouble() <= freq) {
            if (samples.size() < numSamples) {
              samples.add(ReflectionUtils.copy(job.getConfiguration(),
                                               reader.getCurrentValue(), null));
            } else {
              // When exceeding the maximum number of samples, replace a
              // random element with this one, then adjust the frequency
              // to reflect the possibility of existing elements being
              // pushed out
              int ind = r.nextInt(numSamples);
              if (ind != numSamples) {
                samples.set(ind, ReflectionUtils.copy(job.getConfiguration(),
                                 reader.getCurrentValue(), null));
              }
              freq *= (numSamples - 1) / (double) numSamples;
            }
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * Sample from s splits at regular intervals.
   * Useful for sorted data.
   */
  public static class IntervalSampler<K,V> implements Sampler<K,V> {
    protected final double freq;
    protected final int maxSplitsSampled;

    /**
     * Create a new IntervalSampler sampling <em>all</em> splits.
     * @param freq The frequency with which records will be emitted.
     */
    public IntervalSampler(double freq) {
      this(freq, Integer.MAX_VALUE);
    }

    /**
     * Create a new IntervalSampler.
     * @param freq The frequency with which records will be emitted.
     * @param maxSplitsSampled The maximum number of splits to examine.
     * @see #getSample
     */
    public IntervalSampler(double freq, int maxSplitsSampled) {
      this.freq = freq;
      this.maxSplitsSampled = maxSplitsSampled;
    }

    /**
     * For each split sampled, emit when the ratio of the number of records
     * retained to the total record count is less than the specified
     * frequency.
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, Job job) 
        throws IOException, InterruptedException {
    	

      List<InputSplit> splits = inf.getSplits(job);
      ArrayList<V> samples = new ArrayList<V>();
      int splitsToSample = Math.min(maxSplitsSampled, splits.size());
      long records = 0;
      long kept = 0;
      for (int i = 0; i < splitsToSample; ++i) {
        TaskAttemptContext samplingContext = new TaskAttemptContextImpl(
            job.getConfiguration(), new TaskAttemptID());
        RecordReader<K,V> reader = inf.createRecordReader(
            splits.get(i), samplingContext);
        reader.initialize(splits.get(i), samplingContext);
        while (reader.nextKeyValue()) {
          ++records;

          if ((double) kept / records < freq) {
        	  
            samples.add(ReflectionUtils.copy(job.getConfiguration(), reader.getCurrentValue(), null));
            ++kept;
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * Write a partition file for the given job, using the Sampler provided.
   * Queries the sampler for a sample keyset, sorts by the output key
   * comparator, selects the keys for each rank, and writes to the destination
   * returned from {@link TotalOrderPartitioner#getPartitionFile}.
   */
  @SuppressWarnings("unchecked") 
  public static <K,V> void writePartitionFile(Job job, Sampler<K,V> sampler) 
      throws IOException, ClassNotFoundException, InterruptedException {
	  
	    Configuration conf = job.getConfiguration();
	    @SuppressWarnings("rawtypes")
		final InputFormat inf = ReflectionUtils.newInstance(job.getInputFormatClass(), conf);
	    K[] samples = (K[])sampler.getSample(inf, job);
	    LOG.info("Using " + samples.length + " samples");
	    
	    double maxCellPtsPrcnt = conf.getDouble("maxCellPtsPrcnt", 0);
        String[] arr = conf.getStrings("DatasetMBB");
        BoxST DatasetMBB = new BoxST();
        
        if (arr.length == 6){
			
        	DatasetMBB = new BoxST(new PointST(Integer.parseInt(arr[0]), new PointSP(Integer.parseInt(arr[1]),Integer.parseInt(arr[2]))), new PointST(Integer.parseInt(arr[3]), new PointSP(Integer.parseInt(arr[4]),Integer.parseInt(arr[5]))));

        } else if (arr.length == 8){
        	
        	DatasetMBB = new BoxST(new PointST(Integer.parseInt(arr[0]), new PointSP(Integer.parseInt(arr[1]),Integer.parseInt(arr[2]),Integer.parseInt(arr[3]))), new PointST(Integer.parseInt(arr[4]), new PointSP(Integer.parseInt(arr[5]),Integer.parseInt(arr[6]),Integer.parseInt(arr[7]))));

        }
        
	    Path dst = new Path(TotalOrderPartitioner.getPartitionFile(conf));
	    FileSystem fs = dst.getFileSystem(conf);
	    if (fs.exists(dst)) {
	      fs.delete(dst, false);
	    }
	    @SuppressWarnings("deprecation")
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, dst, job.getMapOutputKeyClass(), NullWritable.class);
	    NullWritable nullValue = NullWritable.get();

	    
		String line = new String();
  		line = samples[0].toString();
		StringTokenizer linetokenizer = new StringTokenizer(line, ",");
		int n_of_args = linetokenizer.countTokens();
		
		PointST point = new PointST();

		int NodeID;
		@SuppressWarnings("unused")
		int level;
		
	 	BoxSP cell = new BoxSP();
	 	BoxSP q1 = new BoxSP();
	 	BoxSP q2 = new BoxSP();
	 	BoxSP q3 = new BoxSP();
	 	BoxSP q4 = new BoxSP();
	 	BoxSP q5 = new BoxSP();
	 	BoxSP q6 = new BoxSP();
	 	BoxSP q7 = new BoxSP();
	 	BoxSP q8 = new BoxSP();

	 	OctTreeNode Node = new OctTreeNode();
	 	OctTreeNode qNode = new OctTreeNode();
	 	OctTreeNode q1Node = new OctTreeNode();
	 	OctTreeNode q2Node = new OctTreeNode();
	 	OctTreeNode q3Node = new OctTreeNode();
	 	OctTreeNode q4Node = new OctTreeNode();
	 	OctTreeNode q5Node = new OctTreeNode();
	 	OctTreeNode q6Node = new OctTreeNode();
	 	OctTreeNode q7Node = new OctTreeNode();
		OctTreeNode q8Node = new OctTreeNode();

		OctTreeNode ParentNode = new OctTreeNode();

	 	
	 	int nofExceedingCells;

	 	HashMap<OctTreeNode, ArrayList<PointST>> Mesh = new HashMap<OctTreeNode, ArrayList<PointST>>();
	 	HashMap<OctTreeNode, ArrayList<PointST>> MeshTemp = new HashMap<OctTreeNode, ArrayList<PointST>>();

	 	int nofPntsPerPart = (int)Math.ceil((double)samples.length * maxCellPtsPrcnt);
	 	
	 	if (n_of_args == 5){
			cell = new BoxSP(new PointSP(DatasetMBB.lp.p.x, DatasetMBB.lp.p.y), new PointSP(DatasetMBB.hp.p.x, DatasetMBB.hp.p.y));
			Node = new OctTreeNode(0, cell, 1, 2, 3, 4);
	 	} else {
			cell = new BoxSP(new PointSP(DatasetMBB.lp.p.x, DatasetMBB.lp.p.y, DatasetMBB.lp.p.z), new PointSP(DatasetMBB.hp.p.x, DatasetMBB.hp.p.y, DatasetMBB.hp.p.z));
			Node = new OctTreeNode(0, cell, 1, 2, 3, 4, 5, 6, 7, 8);
	 	}
		ArrayList<PointST> L = new ArrayList<PointST>();
 		Mesh.put(Node, L);

 		for (int j = 0; j < samples.length - 1; j++){
      		line = samples[j].toString();
            linetokenizer = new StringTokenizer(line, ",");
            while (linetokenizer.hasMoreTokens()) {
            	Integer.parseInt(linetokenizer.nextToken());
            	Integer.parseInt(linetokenizer.nextToken());
        	 	if (n_of_args == 5){
	            	point = new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken())));
        	 	} else {
	            	point = new PointST(Integer.parseInt(linetokenizer.nextToken()), new PointSP(Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken()), Integer.parseInt(linetokenizer.nextToken())));
        	 	}
        	 }
            
			Mesh.get(Node).add(point);
		  
 		}
	
		writer.append(new Text(Node.toString()), nullValue);

		NodeID = 0;
		nofExceedingCells = 1;
		level = 0;
		while (nofExceedingCells > 0){
			
			nofExceedingCells = 0;
			level ++;
		 	MeshTemp.clear();
	
			
			for (Entry<OctTreeNode, ArrayList<PointST>> iter : Mesh.entrySet()) {
				
				qNode = iter.getKey();


				if (Mesh.get(qNode).size() > nofPntsPerPart && qNode.cell.BoxSPDiameter() > (double)2){

					nofExceedingCells++;
					if (n_of_args == 5){

						q1 = new BoxSP(new PointSP(qNode.cell.lp.x, qNode.cell.lp.y), new PointSP(qNode.cell.lp.x + (int)Math.round((double)(qNode.cell.hp.x - qNode.cell.lp.x)/2), qNode.cell.lp.y + (int)Math.round((double)(qNode.cell.hp.y - qNode.cell.lp.y)/2)));
					 	q2 = new BoxSP(new PointSP(qNode.cell.lp.x, qNode.cell.lp.y + (int)Math.round((double)(qNode.cell.hp.y - qNode.cell.lp.y)/2)), new PointSP(qNode.cell.lp.x + (int)Math.round((double)(qNode.cell.hp.x - qNode.cell.lp.x)/2), qNode.cell.hp.y));
					 	q3 = new BoxSP(new PointSP(qNode.cell.lp.x + (int)Math.round((double)(qNode.cell.hp.x - qNode.cell.lp.x)/2), qNode.cell.lp.y), new PointSP(qNode.cell.hp.x, qNode.cell.lp.y + (int)Math.round((double)(qNode.cell.hp.y - qNode.cell.lp.y)/2)));
					 	q4 = new BoxSP(new PointSP(qNode.cell.lp.x + (int)Math.round((double)(qNode.cell.hp.x - qNode.cell.lp.x)/2), qNode.cell.lp.y + (int)Math.round((double)(qNode.cell.hp.y - qNode.cell.lp.y)/2)), new PointSP(qNode.cell.hp.x, qNode.cell.hp.y));
					 	
						L = new ArrayList<PointST>();

						q1Node = new OctTreeNode(++NodeID, q1, 0, 0, 0, 0);
						q2Node = new OctTreeNode(++NodeID, q2, 0, 0, 0, 0);
						q3Node = new OctTreeNode(++NodeID, q3, 0, 0, 0, 0);
						q4Node = new OctTreeNode(++NodeID, q4, 0, 0, 0, 0);

						if (iter.getKey().ID > 0){
							
							ParentNode = new OctTreeNode(iter.getKey().ID, iter.getKey().cell, q1Node.ID, q2Node.ID, q3Node.ID, q4Node.ID);
							writer.append(new Text(ParentNode.toString()), nullValue);

						}
						
						MeshTemp.put(q1Node, L);
						MeshTemp.put(q2Node, L);
						MeshTemp.put(q3Node, L);
						MeshTemp.put(q4Node, L);
						
						for (int l = 0; l < Mesh.get(qNode).size(); l++){
							
							if (q1.BoxSPContains(Mesh.get(qNode).get(l).toPointSP())){
								
								MeshTemp.get(q1Node).add(Mesh.get(qNode).get(l));
								
							} else if (q2.BoxSPContains(Mesh.get(qNode).get(l).toPointSP())){
								
								MeshTemp.get(q2Node).add(Mesh.get(qNode).get(l));
								
							} else if (q3.BoxSPContains(Mesh.get(qNode).get(l).toPointSP())){
	
								MeshTemp.get(q3Node).add(Mesh.get(qNode).get(l));
								
							} else if (q4.BoxSPContains(Mesh.get(qNode).get(l).toPointSP())){
								
								MeshTemp.get(q4Node).add(Mesh.get(qNode).get(l));
							} 
						}
	
					} else if (n_of_args == 6){
						
					 	q1 = new BoxSP(new PointSP(qNode.cell.lp.x, qNode.cell.lp.y, qNode.cell.lp.z), new PointSP(qNode.cell.lp.x + (int)Math.round((double)(qNode.cell.hp.x - qNode.cell.lp.x)/2), qNode.cell.lp.y + (int)Math.round((double)(qNode.cell.hp.y - qNode.cell.lp.y)/2), qNode.cell.lp.z + (int)Math.round((double)(qNode.cell.hp.z - qNode.cell.lp.z)/2)));
					 	q2 = new BoxSP(new PointSP(qNode.cell.lp.x, qNode.cell.lp.y + (int)Math.round((double)(qNode.cell.hp.y - qNode.cell.lp.y)/2), qNode.cell.lp.z), new PointSP(qNode.cell.lp.x + (int)Math.round((double)(qNode.cell.hp.x - qNode.cell.lp.x)/2), qNode.cell.hp.y, qNode.cell.lp.z + (int)Math.round((double)(qNode.cell.hp.z - qNode.cell.lp.z)/2)));
					 	q3 = new BoxSP(new PointSP(qNode.cell.lp.x + (int)Math.round((double)(qNode.cell.hp.x - qNode.cell.lp.x)/2), qNode.cell.lp.y, qNode.cell.lp.z), new PointSP(qNode.cell.hp.x, qNode.cell.lp.y + (int)Math.round((double)(qNode.cell.hp.y - qNode.cell.lp.y)/2), qNode.cell.lp.z + (int)Math.round((double)(qNode.cell.hp.z - qNode.cell.lp.z)/2)));
					 	q4 = new BoxSP(new PointSP(qNode.cell.lp.x + (int)Math.round((double)(qNode.cell.hp.x - qNode.cell.lp.x)/2), qNode.cell.lp.y + (int)Math.round((double)(qNode.cell.hp.y - qNode.cell.lp.y)/2), qNode.cell.lp.z), new PointSP(qNode.cell.hp.x, qNode.cell.hp.y, qNode.cell.lp.z + (int)Math.round((double)(qNode.cell.hp.z - qNode.cell.lp.z)/2)));
					 	q5 = new BoxSP(new PointSP(qNode.cell.lp.x, qNode.cell.lp.y, qNode.cell.lp.z + (int)Math.round((double)(qNode.cell.hp.z - qNode.cell.lp.z)/2)), new PointSP(qNode.cell.lp.x + (int)Math.round((double)(qNode.cell.hp.x - qNode.cell.lp.x)/2), qNode.cell.lp.y + (int)Math.round((double)(qNode.cell.hp.y - qNode.cell.lp.y)/2), qNode.cell.hp.z));
					 	q6 = new BoxSP(new PointSP(qNode.cell.lp.x, qNode.cell.lp.y + (int)Math.round((double)(qNode.cell.hp.y - qNode.cell.lp.y)/2), qNode.cell.lp.z + (int)Math.round((double)(qNode.cell.hp.z - qNode.cell.lp.z)/2)), new PointSP(qNode.cell.lp.x + (int)Math.round((double)(qNode.cell.hp.x - qNode.cell.lp.x)/2), qNode.cell.hp.y, qNode.cell.hp.z));
					 	q7 = new BoxSP(new PointSP(qNode.cell.lp.x + (int)Math.round((double)(qNode.cell.hp.x - qNode.cell.lp.x)/2), qNode.cell.lp.y, qNode.cell.lp.z + (int)Math.round((double)(qNode.cell.hp.z - qNode.cell.lp.z)/2)), new PointSP(qNode.cell.hp.x, qNode.cell.lp.y + (int)Math.round((double)(qNode.cell.hp.y - qNode.cell.lp.y)/2), qNode.cell.hp.z));
						q8 = new BoxSP(new PointSP(qNode.cell.lp.x + (int)Math.round((double)(qNode.cell.hp.x - qNode.cell.lp.x)/2), qNode.cell.lp.y + (int)Math.round((double)(qNode.cell.hp.y - qNode.cell.lp.y)/2), qNode.cell.lp.z + (int)Math.round((double)(qNode.cell.hp.z - qNode.cell.lp.z)/2)), new PointSP(qNode.cell.hp.x, qNode.cell.hp.y, qNode.cell.hp.z));

						L = new ArrayList<PointST>();
						q1Node = new OctTreeNode(++NodeID, q1, 0, 0, 0, 0, 0, 0, 0, 0);
						q2Node = new OctTreeNode(++NodeID, q2, 0, 0, 0, 0, 0, 0, 0, 0);
						q3Node = new OctTreeNode(++NodeID, q3, 0, 0, 0, 0, 0, 0, 0, 0);
						q4Node = new OctTreeNode(++NodeID, q4, 0, 0, 0, 0, 0, 0, 0, 0);
						q5Node = new OctTreeNode(++NodeID, q5, 0, 0, 0, 0, 0, 0, 0, 0);
						q6Node = new OctTreeNode(++NodeID, q6, 0, 0, 0, 0, 0, 0, 0, 0);
						q7Node = new OctTreeNode(++NodeID, q7, 0, 0, 0, 0, 0, 0, 0, 0);
						q8Node = new OctTreeNode(++NodeID, q8, 0, 0, 0, 0, 0, 0, 0, 0);
						
						if (iter.getKey().ID > 0){
							
							ParentNode = new OctTreeNode(iter.getKey().ID, iter.getKey().cell, q1Node.ID, q2Node.ID, q3Node.ID, q4Node.ID, q5Node.ID, q6Node.ID, q7Node.ID, q8Node.ID);
							writer.append(new Text(ParentNode.toString()), nullValue);

						}
						
						MeshTemp.put(q1Node, L);
						MeshTemp.put(q2Node, L);
						MeshTemp.put(q3Node, L);
						MeshTemp.put(q4Node, L);
						MeshTemp.put(q5Node, L);
						MeshTemp.put(q6Node, L);
						MeshTemp.put(q7Node, L);
						MeshTemp.put(q8Node, L);
						
						for (int l = 0; l < Mesh.get(qNode).size(); l++){
							
							if (q1.BoxSPContains(Mesh.get(qNode).get(l).toPointSP())){
								
								MeshTemp.get(q1Node).add(Mesh.get(qNode).get(l));
								
							} else if (q2.BoxSPContains(Mesh.get(qNode).get(l).toPointSP())){
								
								MeshTemp.get(q2Node).add(Mesh.get(qNode).get(l));
								
							} else if (q3.BoxSPContains(Mesh.get(qNode).get(l).toPointSP())){
	
								MeshTemp.get(q3Node).add(Mesh.get(qNode).get(l));
								
							} else if (q4.BoxSPContains(Mesh.get(qNode).get(l).toPointSP())){
								
								MeshTemp.get(q4Node).add(Mesh.get(qNode).get(l));
			
							} else if (q5.BoxSPContains(Mesh.get(qNode).get(l).toPointSP())){
								
								MeshTemp.get(q5Node).add(Mesh.get(qNode).get(l));
	
							}else if (q6.BoxSPContains(Mesh.get(qNode).get(l).toPointSP())){
								
								MeshTemp.get(q6Node).add(Mesh.get(qNode).get(l));
	
							}else if (q7.BoxSPContains(Mesh.get(qNode).get(l).toPointSP())){
								
								MeshTemp.get(q7Node).add(Mesh.get(qNode).get(l));
	
							}else if (q8.BoxSPContains(Mesh.get(qNode).get(l).toPointSP())){
								
								MeshTemp.get(q8Node).add(Mesh.get(qNode).get(l));
	
							}
						}
					}
					
				} else {
					
					writer.append(new Text(iter.getKey().toString()), nullValue);

	
				}
			}
			
		
			Mesh.clear();
	
			Mesh.putAll(MeshTemp);
			
	
		}

    writer.close();
  }

  /**
   * Driver for InputSampler from the command line.
   * Configures a JobConf instance and calls {@link #writePartitionFile}.
   */
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf());
    ArrayList<String> otherArgs = new ArrayList<String>();
    Sampler<K,V> sampler = null;
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-r".equals(args[i])) {
          job.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else if ("-inFormat".equals(args[i])) {
          job.setInputFormatClass(
              Class.forName(args[++i]).asSubclass(InputFormat.class));
        } else if ("-keyClass".equals(args[i])) {
          job.setMapOutputKeyClass(
              Class.forName(args[++i]).asSubclass(WritableComparable.class));
        } else if ("-splitSample".equals(args[i])) {
          int numSamples = Integer.parseInt(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler = new SplitSampler<K,V>(numSamples, maxSplits);
        } else if ("-splitRandom".equals(args[i])) {
          double pcnt = Double.parseDouble(args[++i]);
          int numSamples = Integer.parseInt(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler = new RandomSampler<K,V>(pcnt, numSamples, maxSplits);
        } else if ("-splitInterval".equals(args[i])) {
          double pcnt = Double.parseDouble(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler = new IntervalSampler<K,V>(pcnt, maxSplits);
        } else {
          otherArgs.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
            args[i-1]);
        return printUsage();
      }
    }
    if (job.getNumReduceTasks() <= 1) {
      System.err.println("Sampler requires more than one reducer");
      return printUsage();
    }
    if (otherArgs.size() < 2) {
      System.out.println("ERROR: Wrong number of parameters: ");
      return printUsage();
    }
    if (null == sampler) {
      sampler = new RandomSampler<K,V>(0.1, 10000, 10);
    }

    Path outf = new Path(otherArgs.remove(otherArgs.size() - 1));
    TotalOrderPartitioner.setPartitionFile(getConf(), outf);
    for (String s : otherArgs) {
      FileInputFormat.addInputPath(job, new Path(s));
    }
    CreateOctTreePartitionFile.<K,V>writePartitionFile(job, sampler);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    @SuppressWarnings("rawtypes")
	CreateOctTreePartitionFile<?,?> sampler = new CreateOctTreePartitionFile(new Configuration());
    int res = ToolRunner.run(sampler, args);
    System.exit(res);
  }
}
