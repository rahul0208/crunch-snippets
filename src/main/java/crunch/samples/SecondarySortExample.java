package crunch.samples;

import org.apache.commons.lang.StringUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.writable.Writables;

/**
 * The example will split the input parts for \t character. Then it would first
 * sort on the second string and then on the first one. The output would print
 * back a complete string.
 * <p>
 * Sample input args: <i>src/main/resources/hadoopIssues.txt
 * target/hadoop-status</i>
 * <P>
 * This would first sort the issues on their status and then on the id. The
 * result would be written back to the output
 * 
 */
public class SecondarySortExample {
  static class TextTokenizer extends DoFn<String, Pair<String, Pair<String, String>>> {

    @Override
    public void process(String line, Emitter<Pair<String, Pair<String, String>>> emitter) {
      String[] split = StringUtils.split(line, "\t");
      Pair<String, String> data = new Pair<String, String>(split[0], split[2]);
      emitter.emit(new Pair<String, Pair<String, String>>(split[1], data));
    }
  }

  static class TextFormatter extends DoFn<Pair<String, Iterable<Pair<String, String>>>, String> {
    @Override
    public void process(Pair<String, Iterable<Pair<String, String>>> in, Emitter<String> emitter) {
      for (Pair<String, String> pair : in.second()) {
        StringBuilder builder = new StringBuilder();
        builder.append(in.first()).append(":").append(pair.first()).append("\t").append(pair.second());
        emitter.emit(builder.toString());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Pipeline pipeline = new MRPipeline(SecondarySortExample.class);
    PCollection<String> lines = pipeline.readTextFile(args[0]);
    PTable<String, Pair<String, String>> data = lines.parallelDo(new TextTokenizer(),
        Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.strings())));
    PCollection<String> sortedData = SecondarySort.sortAndApply(data, new TextFormatter(), Writables.strings());
    pipeline.writeTextFile(sortedData, args[1]);
    pipeline.done();
  }
}
