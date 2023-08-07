package org.sqlite.jni.tester;
import java.io.*;
import java.nio.charset.StandardCharsets;
//import java.util.List;
//import java.util.ArrayList;

/**
   This class represents a single test script. It handles (or delegates)
   its input and parsing. Iteration and evalution are deferred to other,
   as-yet-non-existent, classes.

*/
public class TestScript {
  //! Test script content.
  private String content;

  private byte[] readFile(String filename) throws IOException {
    return java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(filename));
  }

  /**
     Initializes the script with the content of the given file.
  */
  public TestScript(String filename) throws IOException{
    this.content = new String(readFile(filename),
                              StandardCharsets.UTF_8);
  }

  /**
     Initializes the script with the given content, copied
     at construction-time.
  */
  public TestScript(StringBuffer content){
    this.content = content.toString();
  }
}
