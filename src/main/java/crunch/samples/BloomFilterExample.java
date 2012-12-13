package crunch.samples;

import java.util.Arrays;
import java.util.Collection;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.contrib.bloomfilter.BloomFilterFactory;
import org.apache.crunch.contrib.bloomfilter.BloomFilterFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import crunch.samples.WordCount.Tokenizer;

/**
 * The example can be used to create a bloom filter for a file. After that it
 * checks if the remaining of the arguments are available in the filter or not.
 * <P>
 * Sample input args : <i>Readme.md crunch crunch-snippets india</i>
 * <p>
 * This would create filter for <i>Readme.md</i> file and check the remaining
 * words in it.
 */
public class BloomFilterExample {
  static class TextFilter extends BloomFilterFn<String> {
    @Override
    public Collection<Key> generateKeys(String input) {
      return Arrays.asList(new Key(input.getBytes()));
    }
  }

  public static void main(String[] args) throws Exception {
    Pipeline pipeline = new MRPipeline(BloomFilterExample.class);
    PCollection<String> lines = pipeline.readTextFile(args[0]);
    PCollection<String> words = lines.parallelDo(new Tokenizer(), Writables.strings());
    BloomFilter filter = BloomFilterFactory.createFilter(words, new TextFilter()).getValue();
    for (int count = 1; count < args.length; count++) {
      boolean test = filter.membershipTest(new Key(args[count].getBytes()));
      System.out.println(args[count] + " exists in file ? " + test);
    }
    pipeline.done();
  }

}
