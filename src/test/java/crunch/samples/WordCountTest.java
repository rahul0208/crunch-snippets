package crunch.samples;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.crunch.Emitter;
import org.junit.Test;

import crunch.samples.WordCount.Tokenizer;

public class WordCountTest {
  @Test
  public void tokeniseDoesNotGenerateTokens() {
    Tokenizer tokenizer = new Tokenizer();
    final List<String> data = new ArrayList<String>();
    Emitter<String> emitter = new Emitter<String>() {
      @Override
      public void flush() {
      }

      @Override
      public void emit(String in) {
        data.add(in);
      }
    };
    tokenizer.process("", emitter);
    assertEquals(0, data.size());
  }

  @Test
  public void tokenizerShouldGenerateTokens() {
    Tokenizer tokenizer = new Tokenizer();
    final List<String> data = new ArrayList<String>();
    Emitter<String> emitter = new Emitter<String>() {
      @Override
      public void flush() {
      }

      @Override
      public void emit(String in) {
        data.add(in);
      }
    };
    tokenizer.process("get some token", emitter);
    assertEquals(3, data.size());
  }

}
