package crunch.samples;

import java.util.StringTokenizer;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;

/**
 * A word count example for Apache Crunch, based on Hadoop wordcount example at
 * http://wiki.apache.org/hadoop/WordCount.
 */
public class WordCount {

  @SuppressWarnings("serial")
  static class Tokenizer extends DoFn<String, String> {

    @Override
    public void process(String line, Emitter<String> emitter) {
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        emitter.emit(tokenizer.nextToken());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Pipeline pipeline = new MRPipeline(WordCount.class);
    PCollection<String> lines = pipeline.readTextFile(args[0]);
    PCollection<String> words = lines.parallelDo(new Tokenizer(), Writables.strings());
    PTable<String, Long> counts = words.count();
    pipeline.writeTextFile(counts, args[1]);
    pipeline.done();
  }

}
