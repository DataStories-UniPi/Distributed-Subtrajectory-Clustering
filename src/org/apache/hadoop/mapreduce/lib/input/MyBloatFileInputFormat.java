package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


/**
 * CustomInputformat which implements the createRecordReader of abstract class CombineFileInputFormat
 */

public class MyBloatFileInputFormat extends BloatFileInputFormat<LongWritable,Text>{

    public static class MyBloatFileRecordReader extends RecordReader<LongWritable,Text>{
        private LineRecordReader delegate=null;
        private int idx;

        public MyBloatFileRecordReader(BloatFileSplit split,TaskAttemptContext taskcontext ,Integer idx) throws IOException {
            this.idx=idx;
            delegate = new LineRecordReader();
            
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public float getProgress() {
            try {
                return delegate.getProgress();
            }
            catch(Exception e) {
                return 0;
            }
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext taskcontext) throws IOException {
            BloatFileSplit csplit=(BloatFileSplit)split;
            FileSplit fileSplit = new FileSplit(csplit.getPath(idx), csplit.getOffset(idx), csplit.getLength(idx), csplit.getLocations());
            delegate.initialize(fileSplit, taskcontext);
        }

        @Override
        public LongWritable getCurrentKey() throws IOException,
                InterruptedException {
        	
            return delegate.getCurrentKey();
        }


        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return delegate.getCurrentValue();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return delegate.nextKeyValue();
        }

    }

    @Override
    public RecordReader<LongWritable,Text> createRecordReader(InputSplit split,TaskAttemptContext taskcontext) throws IOException {
        return new BloatFileRecordReader<LongWritable,Text>((BloatFileSplit) split, taskcontext, MyBloatFileRecordReader.class);
    }
}