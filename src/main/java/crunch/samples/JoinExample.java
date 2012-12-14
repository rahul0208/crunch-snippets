package crunch.samples;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Join;
import org.apache.crunch.types.writable.Writables;

/**
 * Sample input args: <i>src/main/resources/hadoopIssues.txt
 * src/main/resources/hadoopVersions.txt target/hadoop-status</i>
 * <P>
 * This would join the versions file with the issues to generate a stsus report
 * of how many issues were fixed in which version
 */
public class JoinExample {
  static class TextTokenizer extends DoFn<String, Pair<String, String>> {

    @Override
    public void process(String line, Emitter<Pair<String, String>> emitter) {
      String[] split = StringUtils.split(line, "\t");
      emitter.emit(new Pair<String, String>(split[0], split[1]));
    }
  }

  static class Identity extends DoFn<Pair<String, String>, Pair<String, String>> {

    @Override
    public void process(Pair<String, String> in, Emitter<Pair<String, String>> emitter) {
      emitter.emit(in);
    }
  }

  static class StatusGen extends CombineFn<String, String> {

    @Override
    public void process(Pair<String, Iterable<String>> in, Emitter<Pair<String, String>> emitter) {
      Map<String, Integer> data = new HashMap<String, Integer>();
      for (String status : in.second()) {
        Integer count = data.get(status);
        if (count == null) {
          count = new Integer(0);
        }
        count = count + 1;
        data.put(status, count);
      }
      for (Entry<String, Integer> value : data.entrySet()) {
        String formattedValue = value.getKey();
        if (!formattedValue.contains("(")) {
          formattedValue = value.getKey() + "(" + value.getValue() + ")";
        }
        emitter.emit(new Pair<String, String>(in.first(), formattedValue));
      }

    }
  }

  public static void main(String[] args) throws Exception {
    Pipeline pipeline = new MRPipeline(JoinExample.class);
    PCollection<String> issues = pipeline.readTextFile(args[0]);
    PCollection<String> versions = pipeline.readTextFile(args[1]);
    PTable<String, String> issuesData = issues.parallelDo(new TextTokenizer(),
        Writables.tableOf(Writables.strings(), Writables.strings()));
    PTable<String, String> versionData = versions.parallelDo(new TextTokenizer(),
        Writables.tableOf(Writables.strings(), Writables.strings()));
    PTable<String, String> versionStatus = Join.fullJoin(versionData, issuesData).values()
        .parallelDo(new Identity(), Writables.tableOf(Writables.strings(), Writables.strings()));
    PTable<String, String> combineValues = versionStatus.groupByKey().combineValues(new StatusGen());
    pipeline.writeTextFile(combineValues, args[2]);
    pipeline.done();
  }
}
