/**
   Strips C- and C++-style comments from stdin, sending the results to
   stdout. It assumes that its input is legal C-like code, and does
   only little error handling.

   It treats string literals as anything starting and ending with
   matching double OR single quotes OR backticks (for use with
   scripting languages which use those). It assumes that a quote
   character within a string which uses the same quote type is escaped
   by a backslash. It should not be used on any code which might
   contain C/C++ comments inside heredocs, and similar constructs, as
   it will strip those out.

   Usage: $0 [--keep-first|-k] < input > output

   The --keep-first (-k) flag tells it to retain the first comment in the
   input stream (which is often a license or attribution block). It
   may be given repeatedly, each one incrementing the number of
   retained comments by one.

   License: Public Domain
   Author: Stephan Beal (stephan@wanderinghorse.net)
*/
#include <stdio.h>
#include <assert.h>
#include <string.h>

#if 1
#define MARKER(pfexp)                                                \
  do{ printf("MARKER: %s:%d:\t",__FILE__,__LINE__);                  \
    printf pfexp;                                                    \
  } while(0)
#else
#define MARKER(exp) if(0) printf
#endif

struct {
  FILE * input;
  FILE * output;
  int rc;
  int keepFirst;
} App = {
  0/*input*/,
  0/*output*/,
  0/*rc*/,
  0/*keepFirst*/
};

void do_it_all(void){
  enum states {
    S_NONE = 0 /* not in comment */,
    S_SLASH1 = 1 /* slash - possibly comment prefix */,
    S_CPP = 2 /* in C++ comment */,
    S_C = 3 /* in C comment */
  };
  int ch, prev = EOF;
  FILE * out = App.output;
  int const slash = '/';
  int const star = '*';
  int line = 1;
  int col = 0;
  enum states state = S_NONE /* current state */;
  int elide = 0 /* true if currently eliding output */;
  int state3Col = -99
    /* huge kludge for odd corner case: */
    /*/ <--- here. state3Col marks the source column in which a C-style
      comment starts, so that it can tell if star-slash inside a
      C-style comment is the end of the comment or is the weird corner
      case marked at the start of _this_ comment block. */;
  for( ; EOF != (ch = fgetc(App.input)); prev = ch,
         ++col){
    switch(state){
      case S_NONE:
        if('\''==ch || '"'==ch || '`'==ch){
          /* Read string literal...
             needed to properly catch comments in strings. */
          int const quote = ch,
            startLine = line, startCol = col;
          int ch2, escaped = 0, endOfString = 0;
          fputc(ch, out);
          for( ++col; !endOfString && EOF != (ch2 = fgetc(App.input));
               ++col ){
            switch(ch2){
              case '\\': escaped = !escaped;
                break;
              case '`':
              case '\'':
              case '"':
                if(!escaped && quote == ch2) endOfString = 1;
                escaped = 0;
                break;
              default:
                escaped = 0;
                break;
            }
            if('\n'==ch2){
              ++line;
              col = 0;
            }
            fputc(ch2, out);
          }
          if(EOF == ch2){
            fprintf(stderr, "Unexpected EOF while reading %s literal "
                    "on line %d column %d.\n",
                    ('\''==ch) ? "char" : "string",
                    startLine, startCol);
            App.rc = 1;
            return;
          }
          break;
        }
        else if(slash == ch){
          /* MARKER(("state 0 ==> 1 @ %d:%d\n", line, col)); */
          state = S_SLASH1;
          break;
        }
        fputc(ch, out);
        break;
      case S_SLASH1: /* 1 slash */
        /* MARKER(("SLASH1 @ %d:%d App.keepFirst=%d\n",
           line, col, App.keepFirst)); */
        switch(ch){
          case '*':
            /* Enter C comment */
            if(App.keepFirst>0){
              elide = 0;
              --App.keepFirst;
            }else{
              elide = 1;
            }
            /*MARKER(("state 1 ==> 3 @ %d:%d\n", line, col));*/
            state = S_C;
            state3Col = col-1;
            if(!elide){
              fputc(prev, out);
              fputc(ch, out);
            }
            break;
          case '/':
            /* Enter C++ comment */
            if(App.keepFirst>0){
              elide = 0;
              --App.keepFirst;
            }else{
              elide = 1;
            }
            /*MARKER(("state 1 ==> 2 @ %d:%d\n", line, col));*/
            state = S_CPP;
            if(!elide){
              fputc(prev, out);
              fputc(ch, out);
            }
            break;
          default:
            /* It wasn't a comment after all. */
            state = S_NONE;
            if(!elide){
              fputc(prev, out);
              fputc(ch, out);
            }
        }
        break;
      case S_CPP: /* C++ comment */
        if('\n' == ch){
          /* MARKER(("state 2 ==> 0 @ %d:%d\n", line, col)); */
          state = S_NONE;
          elide = 0;
        }
        if(!elide){
          fputc(ch, out);
        }
        break;
      case S_C: /* C comment */
        if(!elide){
          fputc(ch, out);
        }
        if(slash == ch){
          if(star == prev){
            /* MARKER(("state 3 ==> 0 @ %d:%d\n", line, col)); */
            /* Corner case which breaks this: */
            /*/ <-- slash there */
            /* That shows up twice in a piece of 3rd-party
               code i use. */
            /* And thus state3Col was introduced :/ */
            if(col!=state3Col+2){
              state = S_NONE;
              elide = 0;
              state3Col = -99;
            }
          }
        }
        break;
      default:
        assert(!"impossible!");
        break;
    }
    if('\n' == ch){
      ++line;
      col = 0;
      state3Col = -99;
    }
  }
}

static void usage(char const *zAppName){
  fprintf(stderr, "Strips C- and C++-style comments from stdin and sends "
          "the results to stdout.\n");
  fprintf(stderr, "Usage: %s [--keep-first|-k] < input > output\n", zAppName);
}

int main( int argc, char const * const * argv ){
  int i;
  for(i = 1; i < argc; ++i){
    char const * zArg = argv[i];
    while( '-'==*zArg ) ++zArg;
    if( 0==strcmp(zArg,"k")
        || 0==strcmp(zArg,"keep-first") ){
      ++App.keepFirst;
    }else{
      usage(argv[0]);
      return 1;
    }
  }
  App.input = stdin;
  App.output = stdout;
  do_it_all();
  return App.rc ? 1 : 0;
}
