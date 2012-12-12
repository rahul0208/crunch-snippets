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
