package crunch.samples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.junit.Test;

import crunch.samples.WordCountV2.AppendValues;
import crunch.samples.WordCountV2.KeyValueExchange;

public class WordCountV2Test {

  class TestCheck {
    boolean done = false;
  }

  @Test
  public void shouldInterchangeValues() {
    KeyValueExchange exchange = new KeyValueExchange();
    final Pair<String, Long> initial = new Pair<String, Long>("first", new Long(1));
    final TestCheck check = new TestCheck();
    exchange.process(initial, new Emitter<Pair<Long, String>>() {
      @Override
      public void flush() {
      }

      @Override
      public void emit(Pair<Long, String> arg0) {
        assertEquals(initial.first(), arg0.second());
        assertEquals(initial.second(), arg0.first());
        check.done = true;
      }
    });
    assertTrue(check.done);
  }

  @Test
  public void shouldAppendValues() {
    AppendValues append = new AppendValues();
    final List<String> text = Arrays.asList("First", "one");
    final Pair<Long, Iterable<String>> initial = new Pair<Long, Iterable<String>>(new Long(1), text);
    final TestCheck check = new TestCheck();
    append.process(initial, new Emitter<Pair<Long, String>>() {
      @Override
      public void flush() {
      }

      @Override
      public void emit(Pair<Long, String> arg0) {
        assertEquals(initial.first(), arg0.first());
        for (String data : text) {
          assertTrue(arg0.second().contains(data));
        }
        check.done = true;
      }
    });
    assertTrue(check.done);
  }

}
