package org.sqlite.jni.tester;
import java.util.List;
import java.util.ArrayList;
import java.io.*;
import java.util.regex.*;
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
  private final Outer outer = new Outer();

  private byte[] readFile(String filename) throws IOException {
    return java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(filename));
  }

  private void setContent(String c){
    content = c;
  }
  /**
     Initializes the script with the content of the given file.
  */
  public TestScript(String filename) throws IOException{
    setContent(new String(readFile(filename),
                          java.nio.charset.StandardCharsets.UTF_8));
  }

  /**
     Initializes the script with the given content, copied
     at construction-time.
  */
  public TestScript(StringBuffer content){
    setContent(content.toString());
  }

  public void setVerbose(boolean b){
    outer.setVerbose(b);
  }

  @SuppressWarnings("unchecked")
  private <T> TestScript verbose(T... vals){
    outer.verbose(vals);
    return this;
  }

  /**
     A quick-and-dirty approach to chopping a script up into individual
     commands. The primary problem with this is that it will remove any
     C-style comments from expected script output, which might or might not
     be a real problem.
   */
  private List<String> chunkContent(String input){
    final String sCComment =
      "[/][*]([*](?![/])|[^*])*[*][/]"
      //"/\\*[^/*]*(?:(?!/\\*|\\*/)[/*][^/*]*)*\\*/"
      ;
    final String s3Dash = "^---+[^\\n]*\\n";
    final String sTclComment = "^#[^\\n]*\\n";
    final String sEmptyLine = "^\\n";
    final List<String> lPats = new ArrayList<>();
    lPats.add(sCComment);
    lPats.add(s3Dash);
    lPats.add(sTclComment);
    lPats.add(sEmptyLine);
    //verbose("Content:").verbose(input).verbose("<EOF>");
    String tmp = input;
    for( String s : lPats ){
      final Pattern p = Pattern.compile(
        s, Pattern.MULTILINE
      );
      final Matcher m = p.matcher(tmp);
      /*verbose("Pattern {{{",p.pattern(),"}}} with flags",
              ""+p.flags(),"matches:"
              );*/
      int n = 0;
      //while( m.find() ) verbose("#"+(++n)+"\t",m.group(0).trim());
      tmp = m.replaceAll("");
    }
    // Chunk the newly-stripped text into individual commands.
    final String sCommand = "^--";
    final List<String> rc = new ArrayList<>();
    final Pattern p = Pattern.compile(
      sCommand, Pattern.MULTILINE
    );
    final Matcher m = p.matcher(tmp);
    int ndxPrev = 0, pos = 0;
    String chunk;
    while( m.find() ){
      pos = m.start();
      chunk = tmp.substring(ndxPrev, pos).trim();
      if( !chunk.isEmpty() ) rc.add( chunk );
      ndxPrev = pos + 2;
    }
    if( ndxPrev != pos + 2 ){
      chunk = tmp.substring(ndxPrev, tmp.length()).trim();
      if( !chunk.isEmpty() ) rc.add( chunk );
    }
    return rc;
  }

  /**
     A debug-only function which dumps the content of the test script
     in some form or other (possibly mangled from its original).
  */
  public void dump(){
    List<String> list = chunkContent(content);
    verbose("script chunked by command:");
    int n = 0;
    for(String c : list){
      verbose("#"+(++n),c);
    }
    verbose("<EOF>");
  }
}
