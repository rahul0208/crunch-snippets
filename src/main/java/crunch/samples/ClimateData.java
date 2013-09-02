package crunch.samples;

import java.text.DateFormatSymbols;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.util.StringUtils;

// data available at data.gov.in
public class ClimateData {

    static class MonthlyRainfallFinder extends DoFn<String, Pair<String, Float>> {
        String[] months = new DateFormatSymbols().getShortMonths();

        @Override
        public void process(String input, Emitter<Pair<String, Float>> emitter) {
            String[] parts = StringUtils.split(input);
            if ("YEAR".equals(parts[0])) {
                return;
            }
            String year = parts[0];
            for (int monthCount = 0; monthCount < 12; monthCount++) {
                float rainfall = Float.parseFloat(parts[monthCount + 1]);
                emitter.emit(new Pair<String, Float>(months[monthCount] + "," + year, rainfall));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Pipeline pipeline = new MRPipeline(SortExample.class);
        PCollection<String> lines = pipeline.readTextFile(args[0]);
        PTable<String, Float> rainfallTable = lines.parallelDo(new MonthlyRainfallFinder(),
                Writables.tableOf(Writables.strings(), Writables.floats()));
        PTable<String, Float> topValues = rainfallTable.top(10);
        pipeline.writeTextFile(topValues, args[1]);
        pipeline.done();
    }

}
