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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.LineReader;

import com.google.common.annotations.VisibleForTesting;


public abstract class BloatFileInputFormat<K, V>
  extends FileInputFormat<K, V> {
	
  // mapping from a rack name to the set of Nodes in the rack 
  //**********Not needed*********
  private HashMap<String, Set<String>> rackToNodes = new HashMap<String, Set<String>>();

 
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
	  return false;
  }
  
  public BloatFileInputFormat() {
  }
  
  @Override
  public List<InputSplit> getSplits(JobContext job) 
    throws IOException {


    // all the files in input set
    List<FileStatus> stats = listStatus(job);

    List<InputSplit> splits = new ArrayList<InputSplit>();

    if (stats.size() == 0) {
      return splits;    
    }


	// create splits for all files
	getMoreSplits(job, stats, splits);

    // free up rackToNodes map
    rackToNodes.clear();
    return splits;    
  }
  private void getMoreSplits(JobContext job, List<FileStatus> stats, List<InputSplit> splits)
	throws IOException {
	Configuration conf = job.getConfiguration();
	int epsilon_t = Integer.parseInt(conf.get("epsilon_t"));

	// all blocks for all the files in input set
	OneFileInfo[] files;
	
	// mapping from a rack name to the list of blocks it has
	  //**********Not needed*********

	HashMap<String, List<OneBlockInfo>> rackToBlocks = new HashMap<String, List<OneBlockInfo>>();
	
	// mapping from a block to the nodes on which it has replicas
	  //**********Not needed*********

	HashMap<OneBlockInfo, String[]> blockToNodes = new HashMap<OneBlockInfo, String[]>();
	
	// mapping from a node to the list of blocks that it contains
	           //**********Not needed*********

	HashMap<String, Set<OneBlockInfo>> nodeToBlocks = new HashMap<String, Set<OneBlockInfo>>();
	
	files = new OneFileInfo[stats.size()];
	if (stats.size() == 0) {
	return; 
	}
	
	// populate all the blocks for all files
	long totLength = 0;
	int i = 0;
	for (FileStatus stat : stats) {
	files[i] = new OneFileInfo(stat, conf, isSplitable(job, stat.getPath()),
	              rackToBlocks, blockToNodes, nodeToBlocks,
	              rackToNodes);
	totLength += files[i].getLength();
	}
	
	createSplits(conf, nodeToBlocks, blockToNodes, rackToBlocks, totLength, epsilon_t, splits);
	}

  @VisibleForTesting
  void createSplits(Configuration conf,
		  			 Map<String, Set<OneBlockInfo>> nodeToBlocks,
                     Map<OneBlockInfo, String[]> blockToNodes,
                     Map<String, List<OneBlockInfo>> rackToBlocks,
                     long totLength,
                     long eps_t,
                     List<InputSplit> splits                     
                    ) throws IOException {
   
	long startOffset;
    long end;
    long pos;
    long linelength;
    FileSystem fs;
    Path path;

    Text block_contents = new Text();
    LineReader reader;
    
    FSDataInputStream fileIn;

    ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
    ArrayList<String> blocks_location = new ArrayList<String>();

      for (Iterator<Map.Entry<OneBlockInfo, String[]>> iter = blockToNodes.entrySet().iterator(); iter.hasNext();) {
        Map.Entry<OneBlockInfo, String[]> one = iter.next();
        
        OneBlockInfo curr_block = one.getKey();
        
        if (curr_block.minT == 0){
        	
		      path = curr_block.onepath;
		      fs = path.getFileSystem(conf);
		      
		      startOffset = curr_block.offset;
		      pos = startOffset;
		      end = startOffset + curr_block.length;
		      
		      //open the file
		      fileIn = fs.open(path);

	          reader = new LineReader(fileIn);

		      while (pos < end){
		    	  
		    	  linelength = reader.readLine(block_contents);


	              String[] trim = block_contents.toString().split(",");
	              long t = Integer.parseInt(trim[2]);

			      curr_block.minT = t;
			      
			      break;

	               
		      }
		      
		      reader.close();
		      fileIn.close();

        }
        
        if (curr_block.maxT == Integer.MAX_VALUE){
        	
		      path = curr_block.onepath;
		      fs = path.getFileSystem(conf);

		      startOffset = curr_block.offset;
		      pos = startOffset;
		      end = startOffset + curr_block.length;
		      
		      //open the file
		      fileIn = fs.open(path);

	          reader = new LineReader(fileIn);

		      while (pos < end){
		    	  
		    	  linelength = reader.readLine(block_contents);

		          if (linelength > 0){
		        		
		              String[] trim = block_contents.toString().split(",");
		              long t = Integer.parseInt(trim[2]);
		              
		              curr_block.maxT = t;
				      
	               
		          } else break;	               
		      }

		      reader.close();
		      fileIn.close();

        }
        
        validBlocks.add(curr_block);
        
        for (Iterator<Map.Entry<OneBlockInfo, String[]>> inner_iter = blockToNodes.entrySet().iterator(); inner_iter.hasNext();) {
        	Map.Entry<OneBlockInfo, String[]> other = inner_iter.next();
            
        	OneBlockInfo other_block = other.getKey();

        	// Check if the two blocks are not the same
        	if (!other_block.equals(curr_block)) {
        		
        		//Check if their lifespans intersect
        		if ((curr_block.minT-eps_t <= other_block.maxT && curr_block.maxT+eps_t >= other_block.maxT) || (curr_block.maxT+eps_t >= other_block.minT && curr_block.minT-eps_t <= other_block.minT) ){
        			
				    other_block = new OneBlockInfo(other_block.onepath, other_block.minT, other_block.maxT,other_block.offset, other_block.length, other_block.hosts, other_block.racks);
        			validBlocks.add(other_block);

        		}
        	}
        }
        Collections.sort(validBlocks, new Comparator<OneBlockInfo>(){
        	
            public int compare(OneBlockInfo o1, OneBlockInfo o2) {
                return Long.compare(o1.minT, o2.minT);
            }
        });
        for (int i = 0; i < validBlocks.size(); i++){
        	blocks_location.add(validBlocks.get(i).hosts[0]);
        };
	    
        addCreatedSplit(splits, blocks_location, validBlocks, curr_block.minT, curr_block.maxT);
        validBlocks.clear();
        blocks_location.clear();
        
      }

  }

  /**
   * Create a single split from the list of blocks specified in validBlocks
   * Add this new split into splitList.
   */
  private void addCreatedSplit(List<InputSplit> splitList, 
                               Collection<String> locations, 
                               ArrayList<OneBlockInfo> validBlocks,
                               long basefile_start,
                               long basefile_end) {
    // create an input split
    Path[] fl = new Path[validBlocks.size()];
    long[] offset = new long[validBlocks.size()];
    long[] length = new long[validBlocks.size()];
    for (int i = 0; i < validBlocks.size(); i++) {
      fl[i] = validBlocks.get(i).onepath; 
      offset[i] = validBlocks.get(i).offset;
      length[i] = validBlocks.get(i).length;
    }
     // add this split to the list that is returned
    BloatFileSplit thissplit = new BloatFileSplit(fl, offset, 
                                   length, locations.toArray(new String[0]), basefile_start, basefile_end);
    splitList.add(thissplit); 
  }

 
  /**
   * This is not implemented yet. 
   */
  public abstract RecordReader<K, V> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException;


  /**
   * information about one file from the File System
   */
  @VisibleForTesting
  static class OneFileInfo {
    private long fileSize;               // size of the file
    private long minT;               // minT of the file
    private long maxT;               // maxT of the file
    private OneBlockInfo[] blocks;       // all blocks in this file

    OneFileInfo(FileStatus stat, Configuration conf,
                boolean isSplitable,
                HashMap<String, List<OneBlockInfo>> rackToBlocks,
                HashMap<OneBlockInfo, String[]> blockToNodes,
                HashMap<String, Set<OneBlockInfo>> nodeToBlocks,
                HashMap<String, Set<String>> rackToNodes
                )
                throws IOException {
      this.fileSize = 0;
      
      //get minT and maxT from filename
      String filename = stat.getPath().getName();
      String[] trim1 = filename.split("-");
      String minTmaxT = trim1[0];
      String[] trim2 = minTmaxT.split(" ");
      this.minT = Integer.parseInt(trim2[0]);
      this.maxT = Integer.parseInt(trim2[1]);


      // get block locations from file system
      BlockLocation[] locations;
      if (stat instanceof LocatedFileStatus) {
        locations = ((LocatedFileStatus) stat).getBlockLocations();
      } else {
        FileSystem fs = stat.getPath().getFileSystem(conf);
        locations = fs.getFileBlockLocations(stat, 0, stat.getLen());
      }
      // create a list of all block and their locations
      if (locations == null) {
        blocks = new OneBlockInfo[0];
      } else {

        if(locations.length == 0 && !stat.isDirectory()) {
          locations = new BlockLocation[] { new BlockLocation() };
        }

        if (!isSplitable) {
          // if the file is not splitable, just create the one block with
          // full file length
          blocks = new OneBlockInfo[1];
          fileSize = stat.getLen();
          blocks[0] = new OneBlockInfo(stat.getPath(), this.minT, this.maxT, 0, fileSize,
              locations[0].getHosts(), locations[0].getTopologyPaths());
        } 
        
        populateBlockInfo(blocks, rackToBlocks, blockToNodes, 
                          nodeToBlocks, rackToNodes);
      }
    }
    
    @VisibleForTesting
    static void populateBlockInfo(OneBlockInfo[] blocks,
                          Map<String, List<OneBlockInfo>> rackToBlocks,
                          Map<OneBlockInfo, String[]> blockToNodes,
                          Map<String, Set<OneBlockInfo>> nodeToBlocks,
                          Map<String, Set<String>> rackToNodes) {
      for (OneBlockInfo oneblock : blocks) {
        // add this block to the block --> node locations map
        blockToNodes.put(oneblock, oneblock.hosts);

        // For blocks that do not have host/rack information,
        // assign to default  rack.
        String[] racks = null;
        if (oneblock.hosts.length == 0) {
          racks = new String[]{NetworkTopology.DEFAULT_RACK};
        } else {
          racks = oneblock.racks;
        }

        // add this block to the rack --> block map
        for (int j = 0; j < racks.length; j++) {
          String rack = racks[j];
          List<OneBlockInfo> blklist = rackToBlocks.get(rack);
          if (blklist == null) {
            blklist = new ArrayList<OneBlockInfo>();
            rackToBlocks.put(rack, blklist);
          }
          blklist.add(oneblock);
          if (!racks[j].equals(NetworkTopology.DEFAULT_RACK)) {
            // Add this host to rackToNodes map
            addHostToRack(rackToNodes, racks[j], oneblock.hosts[j]);
          }
        }

        // add this block to the node --> block map
        for (int j = 0; j < oneblock.hosts.length; j++) {
          String node = oneblock.hosts[j];
          Set<OneBlockInfo> blklist = nodeToBlocks.get(node);
          if (blklist == null) {
            blklist = new LinkedHashSet<OneBlockInfo>();
            nodeToBlocks.put(node, blklist);
          }
          blklist.add(oneblock);
        }
      }
    }

    long getLength() {
      return fileSize;
    }

    OneBlockInfo[] getBlocks() {
      return blocks;
    }
  }

  /**
   * information about one block from the File System
   */
  @VisibleForTesting
  static class OneBlockInfo {
    Path onepath;                // name of this file
    long minT;					 // minT of this file
    long maxT;					 // maxT of this file
    long offset;                 // offset in file
    long length;                 // length of this block
    String[] hosts;              // nodes on which this block resides
    String[] racks;              // network topology of hosts

    OneBlockInfo(Path path, long minT, long maxT, long offset, long len, 
                 String[] hosts, String[] topologyPaths) {
      this.onepath = path;
      this.minT = minT;
      this.maxT = maxT;
      this.offset = offset;
      this.hosts = hosts;
      this.length = len;
      assert (hosts.length == topologyPaths.length ||
              topologyPaths.length == 0);

      // if the file system does not have any rack information, then
      // use dummy rack location.
      if (topologyPaths.length == 0) {
        topologyPaths = new String[hosts.length];
        for (int i = 0; i < topologyPaths.length; i++) {
          topologyPaths[i] = (new NodeBase(hosts[i], 
                              NetworkTopology.DEFAULT_RACK)).toString();
        }
      }

      // The topology paths have the host name included as the last 
      // component. Strip it.
      this.racks = new String[topologyPaths.length];
      for (int i = 0; i < topologyPaths.length; i++) {
        this.racks[i] = (new NodeBase(topologyPaths[i])).getNetworkLocation();
      }
    }
  }

  protected BlockLocation[] getFileBlockLocations(
    FileSystem fs, FileStatus stat) throws IOException {
    if (stat instanceof LocatedFileStatus) {
      return ((LocatedFileStatus) stat).getBlockLocations();
    }
    return fs.getFileBlockLocations(stat, 0, stat.getLen());
  }

  private static void addHostToRack(Map<String, Set<String>> rackToNodes,
                                    String rack, String host) {
    Set<String> hosts = rackToNodes.get(rack);
    if (hosts == null) {
      hosts = new HashSet<String>();
      rackToNodes.put(rack, hosts);
    }
    hosts.add(host);
  }
  
  @SuppressWarnings("unused")
private Set<String> getHosts(Set<String> racks) {
    Set<String> hosts = new HashSet<String>();
    for (String rack : racks) {
      if (rackToNodes.containsKey(rack)) {
        hosts.addAll(rackToNodes.get(rack));
      }
    }
    return hosts;
  }
  
  
}
