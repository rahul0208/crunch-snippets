package crunch.samples;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;

import crunch.samples.WordCount.Tokenizer;

public class WordCountV2 {

  static class KeyValueExchange extends DoFn<Pair<String, Long>, Pair<Long, String>> {
    @Override
    public void process(Pair<String, Long> arg, Emitter<Pair<Long, String>> emitter) {
      emitter.emit(new Pair<Long, String>(arg.second(), arg.first()));
    }
  }

  static class AppendValues extends DoFn<Pair<Long, Iterable<String>>, Pair<Long, String>> {
    @Override
    public void process(Pair<Long, Iterable<String>> arg, Emitter<Pair<Long, String>> emitter) {
      StringBuilder builder = new StringBuilder();
      for (String data : arg.second()) {
        builder.append(data).append(";");
      }
      emitter.emit(new Pair<Long, String>(arg.first(), builder.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Pipeline pipeline = new MRPipeline(WordCountV2.class);
    PCollection<String> lines = pipeline.readTextFile(args[0]);
    PTable<Long, String> data = lines.parallelDo(new Tokenizer(), Writables.strings()).count()
        .parallelDo(new KeyValueExchange(), Writables.tableOf(Writables.longs(), Writables.strings())).groupByKey()
        .parallelDo(new AppendValues(), Writables.tableOf(Writables.longs(), Writables.strings()));
    pipeline.writeTextFile(data, args[1]);
    pipeline.done();
  }
}
