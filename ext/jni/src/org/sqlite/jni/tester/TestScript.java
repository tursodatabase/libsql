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

  /**
     Initializes the script with the content of the given file.
  */
  public TestScript(String filename) throws IOException{
    this.content = new String(readFile(filename),
                              java.nio.charset.StandardCharsets.UTF_8);
  }

  /**
     Initializes the script with the given content, copied
     at construction-time.
  */
  public TestScript(StringBuffer content){
    this.content = content.toString();
  }

  public void setVerbose(boolean b){
    this.outer.setVerbose(b);
  }

  @SuppressWarnings("unchecked")
  private <T> TestScript verbose(T... vals){
    this.outer.verbose(vals);
    return this;
  }

  /**
     A quick-and-dirty approach to chopping a script up into individual
     commands. The primary problem with this is that it will remove any
     C-style comments from expected script output, which might or might not
     be a real problem.
   */
  private String chunkContent(){
    final String sCComment =
      "[/][*]([*](?![/])|[^*])*[*][/]"
      //"/\\*[^/*]*(?:(?!/\\*|\\*/)[/*][^/*]*)*\\*/"
      ;
    final String s3Dash = "^---[^\\n]*\\n";
    final String sTclComment = "^#[^\\n]*\\n";
    final String sEmptyLine = "^\\n";
    final String sCommand = "^--.*$";
    final List<String> lPats = new ArrayList<>();
    lPats.add(sCComment);
    lPats.add(s3Dash);
    lPats.add(sTclComment);
    lPats.add(sEmptyLine);
    //lPats.add(sCommand);
    verbose("Content:").verbose(content).verbose("<EOF>");
    String tmp = content;
    for( String s : lPats ){
      final Pattern p = Pattern.compile(
        s, Pattern.MULTILINE
      );
      final Matcher m = p.matcher(tmp);
      verbose("Pattern {{{",p.pattern(),"}}} with flags",
              ""+p.flags(),"matches:"
      );
      int n = 0;
      while(m.find()){
        verbose("#"+(++n)+"\t",m.group(0).trim());
      }
      tmp = m.replaceAll("");
    }
    //final Pattern patCComments = new Pattern();
    //tmp = content.replace(sCComment,"");
    //tmp = tmp.replace(s3Dash,"");
    //tmp = tmp.replace(sTclComment,"");
    //tmp = tmp.replace(sEmptyLine,"");
    return tmp;
  }

  /**
     A debug-only function which dumps the content of the test script
     in some form or other (possibly mangled from its original).
  */
  public void dump(){
    String s = this.chunkContent();
    this.verbose("chunked script:").verbose(s).verbose("<EOF>");
  }
}
