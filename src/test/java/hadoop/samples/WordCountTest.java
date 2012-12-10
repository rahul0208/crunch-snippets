package hadoop.samples;

import hadoop.samples.WordCount.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

public class WordCountTest {

  @Test
  public void testMapFunction() throws Exception {
    Map mapper = new WordCount.Map();
    Context context = null;
    // context= new Context(conf, taskid, reader, writer, committer, reporter, split) ???
    mapper.map(new LongWritable(), new Text(), context);
    //assert ????
   }

}
