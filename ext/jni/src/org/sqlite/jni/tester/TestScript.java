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
  private String name;
  private String content;
  private List<String> chunks = null;
  private final Outer outer = new Outer();
  private boolean ignored = false;

  private byte[] readFile(String filename) throws Exception {
    return java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(filename));
  }

  private void setContent(String c){
    content = c;
    ignored = shouldBeIgnored(c);
    chunks = chunkContent();
  }
  /**
     Initializes the script with the content of the given file.
  */
  public TestScript(String filename) throws Exception{
    setContent(new String(readFile(filename),
                          java.nio.charset.StandardCharsets.UTF_8));
    name = filename;
  }

  /**
     Initializes the script with the given content, copied at
     construction-time. The first argument is a filename for that
     content. It need not refer to a real file - it's for display
     purposes only.
  */
  public TestScript(String virtualName, StringBuffer content)
    throws RuntimeException {
    setContent(content.toString());
    name = virtualName;
  }

  public String getName(){
    return name;
  }

  public boolean isIgnored(){
    return ignored;
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
     Returns true if the given script content should be ignored
     (because it contains certain content which indicates such).
  */
  public static boolean shouldBeIgnored(String content){
    return content.indexOf("SCRIPT_MODULE_NAME")>=0
      || content.indexOf("\n|")>=0;
  }

  /**
     A quick-and-dirty approach to chopping a script up into individual
     commands and their inputs.
  */
  private List<String> chunkContent(){
    if( ignored ) return null;
    // First, strip out any content which we know we can ignore...
    final String sCComment = "[/][*]([*](?![/])|[^*])*[*][/]";
    final String s3Dash = "^---+[^\\n]*\\n";
    final String sTclComment = "^#[^\\n]*\\n";
    final String sEmptyLine = "^\\n";
    final List<String> lPats = new ArrayList<>();
    lPats.add(sCComment);
    lPats.add(s3Dash);
    lPats.add(sTclComment);
    lPats.add(sEmptyLine);
    //verbose("Content:").verbose(content).verbose("<EOF>");
    String tmp = content;
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
    // Chunk the newly-cleaned text into individual commands and their input...
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
    if( null==chunks ){
      verbose("This contains content which forces it to be ignored.");
    }else{
      verbose("script commands:");
      int n = 0;
      for(String c : chunks){
        verbose("#"+(++n),c);
      }
      verbose("<EOF>");
    }
  }
}
