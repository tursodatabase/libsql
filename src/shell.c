/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code to implement the "sqlite" command line
** utility for accessing SQLite databases.
**
** $Id: shell.c,v 1.55 2002/04/19 12:34:06 drh Exp $
*/
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "sqlite.h"
#include <ctype.h>

#if !defined(_WIN32) && !defined(WIN32)
# include <signal.h>
# include <pwd.h>
# include <unistd.h>
# include <sys/types.h>
#endif

#if defined(HAVE_READLINE) && HAVE_READLINE==1
# include <readline/readline.h>
# include <readline/history.h>
#else
# define readline(p) getline(p,stdin)
# define add_history(X)
# define read_history(X)
# define write_history(X)
# define stifle_history(X)
#endif

/*
** The following is the open SQLite database.  We make a pointer
** to this database a static variable so that it can be accessed
** by the SIGINT handler to interrupt database processing.
*/
static sqlite *db = 0;

/*
** True if an interrupt (Control-C) has been received.
*/
static int seenInterrupt = 0;

/*
** This is the name of our program. It is set in main(), used
** in a number of other places, mostly for error messages.
*/
static char *Argv0;

/*
** Prompt strings. Initialized in main. Settable with
**   .prompt main continue
*/
static char mainPrompt[20];     /* First line prompt. default: "sqlite> "*/
static char continuePrompt[20]; /* Continuation prompt. default: "   ...> " */

/*
** This routine reads a line of text from standard input, stores
** the text in memory obtained from malloc() and returns a pointer
** to the text.  NULL is returned at end of file, or if malloc()
** fails.
**
** The interface is like "readline" but no command-line editing
** is done.
*/
static char *getline(char *zPrompt, FILE *in){
  char *zLine;
  int nLine;
  int n;
  int eol;

  if( zPrompt && *zPrompt ){
    printf("%s",zPrompt);
    fflush(stdout);
  }
  nLine = 100;
  zLine = malloc( nLine );
  if( zLine==0 ) return 0;
  n = 0;
  eol = 0;
  while( !eol ){
    if( n+100>nLine ){
      nLine = nLine*2 + 100;
      zLine = realloc(zLine, nLine);
      if( zLine==0 ) return 0;
    }
    if( fgets(&zLine[n], nLine - n, in)==0 ){
      if( n==0 ){
        free(zLine);
        return 0;
      }
      zLine[n] = 0;
      eol = 1;
      break;
    }
    while( zLine[n] ){ n++; }
    if( n>0 && zLine[n-1]=='\n' ){
      n--;
      zLine[n] = 0;
      eol = 1;
    }
  }
  zLine = realloc( zLine, n+1 );
  return zLine;
}

/*
** Retrieve a single line of input text.  "isatty" is true if text
** is coming from a terminal.  In that case, we issue a prompt and
** attempt to use "readline" for command-line editing.  If "isatty"
** is false, use "getline" instead of "readline" and issue no prompt.
**
** zPrior is a string of prior text retrieved.  If not the empty
** string, then issue a continuation prompt.
*/
static char *one_input_line(const char *zPrior, FILE *in){
  char *zPrompt;
  char *zResult;
  if( in!=0 ){
    return getline(0, in);
  }
  if( zPrior && zPrior[0] ){
    zPrompt = continuePrompt;
  }else{
    zPrompt = mainPrompt;
  }
  zResult = readline(zPrompt);
  if( zResult ) add_history(zResult);
  return zResult;
}

struct previous_mode_data {
  int valid;        /* Is there legit data in here? */
  int mode;
  int showHeader;
  int colWidth[100];
};
/*
** An pointer to an instance of this structure is passed from
** the main program to the callback.  This is used to communicate
** state and mode information.
*/
struct callback_data {
  sqlite *db;            /* The database */
  int echoOn;            /* True to echo input commands */
  int cnt;               /* Number of records displayed so far */
  FILE *out;             /* Write results here */
  int mode;              /* An output mode setting */
  int showHeader;        /* True to show column names in List or Column mode */
  char *zDestTable;      /* Name of destination table when MODE_Insert */
  char separator[20];    /* Separator character for MODE_List */
  int colWidth[100];     /* Requested width of each column when in column mode*/
  int actualWidth[100];  /* Actual width of each column */
  char nullvalue[20];    /* The text to print when a NULL comes back from the database */
  struct previous_mode_data explainPrev;
                         /* Holds the mode information just before .explain ON */
  char outfile[FILENAME_MAX];
                         /* Filename for *out */
};

/*
** These are the allowed modes.
*/
#define MODE_Line     0  /* One column per line.  Blank line between records */
#define MODE_Column   1  /* One record per line in neat columns */
#define MODE_List     2  /* One record per line with a separator */
#define MODE_Semi     3  /* Same as MODE_List but append ";" to each line */
#define MODE_Html     4  /* Generate an XHTML table */
#define MODE_Insert   5  /* Generate SQL "insert" statements */
#define MODE_NUM_OF   6

char *modeDescr[MODE_NUM_OF] = {
  "line",
  "column",
  "list",
  "semi",
  "html",
  "insert"
};

/*
** Number of elements in an array
*/
#define ArraySize(X)  (sizeof(X)/sizeof(X[0]))

/*
** Return TRUE if the string supplied is a number of some kinds.
*/
static int is_numeric(const char *z){
  int seen_digit = 0;
  if( *z=='-' || *z=='+' ){
    z++;
  }
  while( isdigit(*z) ){
    seen_digit = 1;
    z++;
  }
  if( seen_digit && *z=='.' ){
    z++;
    while( isdigit(*z) ){ z++; }
  }
  if( seen_digit && (*z=='e' || *z=='E')
   && (isdigit(z[1]) || ((z[1]=='-' || z[1]=='+') && isdigit(z[2])))
  ){
    z+=2;
    while( isdigit(*z) ){ z++; }
  }
  return seen_digit && *z==0;
}

/*
** Output the given string as a quoted string using SQL quoting conventions.
*/
static void output_quoted_string(FILE *out, const char *z){
  int i;
  int nSingle = 0;
  int nDouble = 0;
  for(i=0; z[i]; i++){
    if( z[i]=='\'' ) nSingle++;
    else if( z[i]=='"' ) nDouble++;
  }
  if( nSingle==0 ){
    fprintf(out,"'%s'",z);
  }else if( nDouble==0 ){
    fprintf(out,"\"%s\"",z);
  }else{
    fprintf(out,"'");
    while( *z ){
      for(i=0; z[i] && z[i]!='\''; i++){}
      if( i==0 ){
        fprintf(out,"''");
        z++;
      }else if( z[i]=='\'' ){
        fprintf(out,"%.*s''",i,z);
        z += i+1;
      }else{
        fprintf(out,"%s",z);
        break;
      }
    }
    fprintf(out,"'");
  }
}

/*
** Output the given string with characters that are special to
** HTML escaped.
*/
static void output_html_string(FILE *out, const char *z){
  int i;
  while( *z ){
    for(i=0; z[i] && z[i]!='<' && z[i]!='&'; i++){}
    if( i>0 ){
      fprintf(out,"%.*s",i,z);
    }
    if( z[i]=='<' ){
      fprintf(out,"&lt;");
    }else if( z[i]=='&' ){
      fprintf(out,"&amp;");
    }else{
      break;
    }
    z += i + 1;
  }
}

/*
** This routine runs when the user presses Ctrl-C
*/
static void interrupt_handler(int NotUsed){
  seenInterrupt = 1;
  if( db ) sqlite_interrupt(db);
}

/*
** This is the callback routine that the SQLite library
** invokes for each row of a query result.
*/
static int callback(void *pArg, int nArg, char **azArg, char **azCol){
  int i;
  struct callback_data *p = (struct callback_data*)pArg;
  switch( p->mode ){
    case MODE_Line: {
      int w = 5;
      if( azArg==0 ) break;
      for(i=0; i<nArg; i++){
        int len = strlen(azCol[i]);
        if( len>w ) w = len;
      }
      if( p->cnt++>0 ) fprintf(p->out,"\n");
      for(i=0; i<nArg; i++){
        fprintf(p->out,"%*s = %s\n", w, azCol[i], azArg[i] ? azArg[i] : p->nullvalue);
      }
      break;
    }
    case MODE_Column: {
      if( p->cnt++==0 ){
        for(i=0; i<nArg; i++){
          int w, n;
          if( i<ArraySize(p->colWidth) ){
             w = p->colWidth[i];
          }else{
             w = 0;
          }
          if( w<=0 ){
            w = strlen(azCol[i] ? azCol[i] : "");
            if( w<10 ) w = 10;
            n = strlen(azArg && azArg[i] ? azArg[i] : p->nullvalue);
            if( w<n ) w = n;
          }
          if( i<ArraySize(p->actualWidth) ){
            p->actualWidth[i] = w;
          }
          if( p->showHeader ){
            fprintf(p->out,"%-*.*s%s",w,w,azCol[i], i==nArg-1 ? "\n": "  ");
          }
        }
        if( p->showHeader ){
          for(i=0; i<nArg; i++){
            int w;
            if( i<ArraySize(p->actualWidth) ){
               w = p->actualWidth[i];
            }else{
               w = 10;
            }
            fprintf(p->out,"%-*.*s%s",w,w,"-----------------------------------"
                   "----------------------------------------------------------",
                    i==nArg-1 ? "\n": "  ");
          }
        }
      }
      if( azArg==0 ) break;
      for(i=0; i<nArg; i++){
        int w;
        if( i<ArraySize(p->actualWidth) ){
           w = p->actualWidth[i];
        }else{
           w = 10;
        }
        fprintf(p->out,"%-*.*s%s",w,w,
            azArg[i] ? azArg[i] : p->nullvalue, i==nArg-1 ? "\n": "  ");
      }
      break;
    }
    case MODE_Semi:
    case MODE_List: {
      if( p->cnt++==0 && p->showHeader ){
        for(i=0; i<nArg; i++){
          fprintf(p->out,"%s%s",azCol[i], i==nArg-1 ? "\n" : p->separator);
        }
      }
      if( azArg==0 ) break;
      for(i=0; i<nArg; i++){
        char *z = azArg[i];
        if( z==0 ) z = p->nullvalue;
        fprintf(p->out, "%s", z);
        if( i<nArg-1 ){
          fprintf(p->out, "%s", p->separator);
        }else if( p->mode==MODE_Semi ){
          fprintf(p->out, ";\n");
        }else{
          fprintf(p->out, "\n");
        }
      }
      break;
    }
    case MODE_Html: {
      if( p->cnt++==0 && p->showHeader ){
        fprintf(p->out,"<TR>");
        for(i=0; i<nArg; i++){
          fprintf(p->out,"<TH>%s</TH>",azCol[i]);
        }
        fprintf(p->out,"</TR>\n");
      }
      if( azArg==0 ) break;
      fprintf(p->out,"<TR>");
      for(i=0; i<nArg; i++){
        fprintf(p->out,"<TD>");
        output_html_string(p->out, azArg[i] ? azArg[i] : p->nullvalue);
        fprintf(p->out,"</TD>\n");
      }
      fprintf(p->out,"</TD></TR>\n");
      break;
    }
    case MODE_Insert: {
      if( azArg==0 ) break;
      fprintf(p->out,"INSERT INTO %s VALUES(",p->zDestTable);
      for(i=0; i<nArg; i++){
        char *zSep = i>0 ? ",": "";
        if( azArg[i]==0 ){
          fprintf(p->out,"%sNULL",zSep);
        }else if( is_numeric(azArg[i]) ){
          fprintf(p->out,"%s%s",zSep, azArg[i]);
        }else{
          if( zSep[0] ) fprintf(p->out,"%s",zSep);
          output_quoted_string(p->out, azArg[i]);
        }
      }
      fprintf(p->out,");\n");
      break;
    }
  }
  return 0;
}

/*
** Set the destination table field of the callback_data structure to
** the name of the table given.  Escape any quote characters in the
** table name.
*/
static void set_table_name(struct callback_data *p, const char *zName){
  int i, n;
  int needQuote;
  char *z;

  if( p->zDestTable ){
    free(p->zDestTable);
    p->zDestTable = 0;
  }
  if( zName==0 ) return;
  needQuote = !isalpha(*zName) && *zName!='_';
  for(i=n=0; zName[i]; i++, n++){
    if( !isalnum(zName[i]) && zName[i]!='_' ){
      needQuote = 1;
      if( zName[i]=='\'' ) n++;
    }
  }
  if( needQuote ) n += 2;
  z = p->zDestTable = malloc( n+1 );
  if( z==0 ){
    fprintf(stderr,"Out of memory!\n");
    exit(1);
  }
  n = 0;
  if( needQuote ) z[n++] = '\'';
  for(i=0; zName[i]; i++){
    z[n++] = zName[i];
    if( zName[i]=='\'' ) z[n++] = '\'';
  }
  if( needQuote ) z[n++] = '\'';
  z[n] = 0;
}

/*
** This is a different callback routine used for dumping the database.
** Each row received by this callback consists of a table name,
** the table type ("index" or "table") and SQL to create the table.
** This routine should print text sufficient to recreate the table.
*/
static int dump_callback(void *pArg, int nArg, char **azArg, char **azCol){
  struct callback_data *p = (struct callback_data *)pArg;
  if( nArg!=3 ) return 1;
  fprintf(p->out, "%s;\n", azArg[2]);
  if( strcmp(azArg[1],"table")==0 ){
    struct callback_data d2;
    d2 = *p;
    d2.mode = MODE_Insert;
    d2.zDestTable = 0;
    set_table_name(&d2, azArg[0]);
    sqlite_exec_printf(p->db,
       "SELECT * FROM '%q'",
       callback, &d2, 0, azArg[0]
    );
    set_table_name(&d2, 0);
  }
  return 0;
}

/*
** Text of a help message
*/
static char zHelp[] =
  ".dump ?TABLE? ...      Dump the database in an text format\n"
  ".echo ON|OFF           Turn command echo on or off\n"
  ".exit                  Exit this program\n"
  ".explain ON|OFF        Turn output mode suitable for EXPLAIN on or off.\n"
  ".header(s) ON|OFF      Turn display of headers on or off\n"
  ".help                  Show this message\n"
  ".indices TABLE         Show names of all indices on TABLE\n"
  ".mode MODE             Set mode to one of \"line(s)\", \"column(s)\", \n"
  "                       \"insert\", \"list\", or \"html\"\n"
  ".mode insert TABLE     Generate SQL insert statements for TABLE\n"
  ".nullvalue STRING      Print STRING instead of nothing for NULL data\n"
  ".output FILENAME       Send output to FILENAME\n"
  ".output stdout         Send output to the screen\n"
  ".prompt MAIN CONTINUE  Replace the standard prompts\n"
  ".quit                  Exit this program\n"
  ".read FILENAME         Execute SQL in FILENAME\n"
  ".reindex ?TABLE?       Rebuild indices\n"
/*  ".rename OLD NEW        Change the name of a table or index\n" */
  ".schema ?TABLE?        Show the CREATE statements\n"
  ".separator STRING      Change separator string for \"list\" mode\n"
  ".show                  Show the current values for various settings\n"
  ".tables ?PATTERN?      List names of tables matching a pattern\n"
  ".timeout MS            Try opening locked tables for MS milliseconds\n"
  ".width NUM NUM ...     Set column widths for \"column\" mode\n"
;

/* Forward reference */
static void process_input(struct callback_data *p, FILE *in);

/*
** If an input line begins with "." then invoke this routine to
** process that line.
**
** Return 1 to exit and 0 to continue.
*/
static int do_meta_command(char *zLine, sqlite *db, struct callback_data *p){
  int i = 1;
  int nArg = 0;
  int n, c;
  int rc = 0;
  char *azArg[50];

  /* Parse the input line into tokens.
  */
  while( zLine[i] && nArg<ArraySize(azArg) ){
    while( isspace(zLine[i]) ){ i++; }
    if( zLine[i]=='\'' || zLine[i]=='"' ){
      int delim = zLine[i++];
      azArg[nArg++] = &zLine[i];
      while( zLine[i] && zLine[i]!=delim ){ i++; }
      if( zLine[i]==delim ){
        zLine[i++] = 0;
      }
    }else{
      azArg[nArg++] = &zLine[i];
      while( zLine[i] && !isspace(zLine[i]) ){ i++; }
      if( zLine[i] ) zLine[i++] = 0;
    }
  }

  /* Process the input line.
  */
  if( nArg==0 ) return rc;
  n = strlen(azArg[0]);
  c = azArg[0][0];
  if( c=='d' && strncmp(azArg[0], "dump", n)==0 ){
    char *zErrMsg = 0;
    fprintf(p->out, "BEGIN TRANSACTION;\n");
    if( nArg==1 ){
      sqlite_exec(db,
        "SELECT name, type, sql FROM sqlite_master "
        "WHERE type!='meta' AND sql NOT NULL "
        "ORDER BY substr(type,2,1), name",
        dump_callback, p, &zErrMsg
      );
    }else{
      int i;
      for(i=1; i<nArg && zErrMsg==0; i++){
        sqlite_exec_printf(db,
          "SELECT name, type, sql FROM sqlite_master "
          "WHERE tbl_name LIKE '%q' AND type!='meta' AND sql NOT NULL "
          "ORDER BY substr(type,2,1), name",
          dump_callback, p, &zErrMsg, azArg[i]
        );
      }
    }
    if( zErrMsg ){
      fprintf(stderr,"Error: %s\n", zErrMsg);
      free(zErrMsg);
    }else{
      fprintf(p->out, "COMMIT;\n");
    }
  }else

  if( c=='e' && strncmp(azArg[0], "echo", n)==0 && nArg>1 ){
    int j;
    char *z = azArg[1];
    int val = atoi(azArg[1]);
    for(j=0; z[j]; j++){
      if( isupper(z[j]) ) z[j] = tolower(z[j]);
    }
    if( strcmp(z,"on")==0 ){
      val = 1;
    }else if( strcmp(z,"yes")==0 ){
      val = 1;
    }
    p->echoOn = val;
  }else

  if( c=='e' && strncmp(azArg[0], "exit", n)==0 ){
    rc = 1;
  }else

  if( c=='e' && strncmp(azArg[0], "explain", n)==0 ){
    int j;
    char *z = nArg>=2 ? azArg[1] : "1";
    int val = atoi(z);
    for(j=0; z[j]; j++){
      if( isupper(z[j]) ) z[j] = tolower(z[j]);
    }
    if( strcmp(z,"on")==0 ){
      val = 1;
    }else if( strcmp(z,"yes")==0 ){
      val = 1;
    }
    if(val == 1) {
      if(!p->explainPrev.valid) {
        p->explainPrev.valid = 1;
        p->explainPrev.mode = p->mode;
        p->explainPrev.showHeader = p->showHeader;
        memcpy(p->explainPrev.colWidth,p->colWidth,sizeof(p->colWidth));
      }
      /* We could put this code under the !p->explainValid
      ** condition so that it does not execute if we are already in
      ** explain mode. However, always executing it allows us an easy
      ** was to reset to explain mode in case the user previously
      ** did an .explain followed by a .width, .mode or .header
      ** command.
      */
      p->mode = MODE_Column;
      p->showHeader = 1;
      memset(p->colWidth,0,ArraySize(p->colWidth));
      p->colWidth[0] = 4;
      p->colWidth[1] = 12;
      p->colWidth[2] = 10;
      p->colWidth[3] = 10;
      p->colWidth[4] = 35;
    }else if (p->explainPrev.valid) {
      p->explainPrev.valid = 0;
      p->mode = p->explainPrev.mode;
      p->showHeader = p->explainPrev.showHeader;
      memcpy(p->colWidth,p->explainPrev.colWidth,sizeof(p->colWidth));
    }
  }else

  if( c=='h' && (strncmp(azArg[0], "header", n)==0
                 ||
                 strncmp(azArg[0], "headers", n)==0 )&& nArg>1 ){
    int j;
    char *z = azArg[1];
    int val = atoi(azArg[1]);
    for(j=0; z[j]; j++){
      if( isupper(z[j]) ) z[j] = tolower(z[j]);
    }
    if( strcmp(z,"on")==0 ){
      val = 1;
    }else if( strcmp(z,"yes")==0 ){
      val = 1;
    }
    p->showHeader = val;
  }else

  if( c=='h' && strncmp(azArg[0], "help", n)==0 ){
    fprintf(stderr,zHelp);
  }else

  if( c=='i' && strncmp(azArg[0], "indices", n)==0 && nArg>1 ){
    struct callback_data data;
    char *zErrMsg = 0;
    memcpy(&data, p, sizeof(data));
    data.showHeader = 0;
    data.mode = MODE_List;
    sqlite_exec_printf(db,
      "SELECT name FROM sqlite_master "
      "WHERE type='index' AND tbl_name LIKE '%q' "
      "ORDER BY name",
      callback, &data, &zErrMsg, azArg[1]
    );
    if( zErrMsg ){
      fprintf(stderr,"Error: %s\n", zErrMsg);
      free(zErrMsg);
    }
  }else

  if( c=='m' && strncmp(azArg[0], "mode", n)==0 && nArg>=2 ){
    int n2 = strlen(azArg[1]);
    if( strncmp(azArg[1],"line",n2)==0
        ||
        strncmp(azArg[1],"lines",n2)==0 ){
      p->mode = MODE_Line;
    }else if( strncmp(azArg[1],"column",n2)==0
              ||
              strncmp(azArg[1],"columns",n2)==0 ){
      p->mode = MODE_Column;
    }else if( strncmp(azArg[1],"list",n2)==0 ){
      p->mode = MODE_List;
    }else if( strncmp(azArg[1],"html",n2)==0 ){
      p->mode = MODE_Html;
    }else if( strncmp(azArg[1],"insert",n2)==0 ){
      p->mode = MODE_Insert;
      if( nArg>=3 ){
        set_table_name(p, azArg[2]);
      }else{
        set_table_name(p, "table");
      }
    }else {
      fprintf(stderr,"mode should be on of: column html insert line list\n");
    }
  }else

  if( c=='n' && strncmp(azArg[0], "nullvalue", n)==0 && nArg==2 ) {
    sprintf(p->nullvalue, "%.*s", (int)ArraySize(p->nullvalue)-1, azArg[1]);
  }else

  if( c=='o' && strncmp(azArg[0], "output", n)==0 && nArg==2 ){
    if( p->out!=stdout ){
      fclose(p->out);
    }
    if( strcmp(azArg[1],"stdout")==0 ){
      p->out = stdout;
      strcpy(p->outfile,"stdout");
    }else{
      p->out = fopen(azArg[1], "w");
      if( p->out==0 ){
        fprintf(stderr,"can't write to \"%s\"\n", azArg[1]);
        p->out = stdout;
      } else {
         strcpy(p->outfile,azArg[1]);
      }
    }
  }else

  if( c=='p' && strncmp(azArg[0], "prompt", n)==0 && (nArg==2 || nArg==3)){
    if( nArg >= 2) {
      strncpy(mainPrompt,azArg[1],(int)ArraySize(mainPrompt)-1);
    }
    if( nArg >= 3) {
      strncpy(continuePrompt,azArg[2],(int)ArraySize(continuePrompt)-1);
    }
  }else

  if( c=='q' && strncmp(azArg[0], "quit", n)==0 ){
    sqlite_close(db);
    exit(0);
  }else

  if( c=='r' && strncmp(azArg[0], "read", n)==0 && nArg==2 ){
    FILE *alt = fopen(azArg[1], "r");
    if( alt==0 ){
      fprintf(stderr,"can't open \"%s\"\n", azArg[1]);
    }else{
      process_input(p, alt);
      fclose(alt);
    }
  }else

  if( c=='r' && strncmp(azArg[0], "reindex", n)==0 ){
    char **azResult;
    int nRow, rc;
    char *zErrMsg;
    int i;
    char *zSql;
    if( nArg==1 ){
      rc = sqlite_get_table(db,
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='index'",
        &azResult, &nRow, 0, &zErrMsg
      );
    }else{
      rc = sqlite_get_table_printf(db,
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='index' AND tbl_name LIKE '%q'",
        &azResult, &nRow, 0, &zErrMsg, azArg[1]
      );
    }
    for(i=1; rc==SQLITE_OK && i<=nRow; i++){
      extern char *sqlite_mprintf(const char *, ...);
      zSql = sqlite_mprintf(
         "DROP INDEX '%q';\n%s;\nVACUUM '%q';",
         azResult[i*2], azResult[i*2+1], azResult[i*2]);
      if( p->echoOn ) printf("%s\n", zSql);
      rc = sqlite_exec(db, zSql, 0, 0, &zErrMsg);
    }
    sqlite_free_table(azResult);
    if( zErrMsg ){
      fprintf(stderr,"Error: %s\n", zErrMsg);
      free(zErrMsg);
    }
  }else

  if( c=='s' && strncmp(azArg[0], "schema", n)==0 ){
    struct callback_data data;
    char *zErrMsg = 0;
    memcpy(&data, p, sizeof(data));
    data.showHeader = 0;
    data.mode = MODE_Semi;
    if( nArg>1 ){
      extern int sqliteStrICmp(const char*,const char*);
      if( sqliteStrICmp(azArg[1],"sqlite_master")==0 ){
        char *new_argv[2], *new_colv[2];
        new_argv[0] = "CREATE TABLE sqlite_master (\n"
                      "  type text,\n"
                      "  name text,\n"
                      "  tbl_name text,\n"
                      "  rootpage integer,\n"
                      "  sql text\n"
                      ")";
        new_argv[1] = 0;
        new_colv[0] = "sql";
        new_colv[1] = 0;
        callback(&data, 1, new_argv, new_colv);
      }else{
        sqlite_exec_printf(db,
          "SELECT sql FROM sqlite_master "
          "WHERE tbl_name LIKE '%q' AND type!='meta' AND sql NOTNULL "
          "ORDER BY type DESC, name",
          callback, &data, &zErrMsg, azArg[1]);
      }
    }else{
      sqlite_exec(db,
         "SELECT sql FROM sqlite_master "
         "WHERE type!='meta' AND sql NOTNULL "
         "ORDER BY tbl_name, type DESC, name",
         callback, &data, &zErrMsg
      );
    }
    if( zErrMsg ){
      fprintf(stderr,"Error: %s\n", zErrMsg);
      free(zErrMsg);
    }
  }else

  if( c=='s' && strncmp(azArg[0], "separator", n)==0 && nArg==2 ){
    sprintf(p->separator, "%.*s", (int)ArraySize(p->separator)-1, azArg[1]);
  }else

  if( c=='s' && strncmp(azArg[0], "show", n)==0){
    int i;
    fprintf(p->out,"%9.9s: %s\n","echo", p->echoOn ? "on" : "off");
    fprintf(p->out,"%9.9s: %s\n","explain", p->explainPrev.valid ? "on" :"off");
    fprintf(p->out,"%9.9s: %s\n","headers", p->showHeader ? "on" : "off");
    fprintf(p->out,"%9.9s: %s\n","mode", modeDescr[p->mode]);
    fprintf(p->out,"%9.9s: %s\n","nullvalue", p->nullvalue);
    fprintf(p->out,"%9.9s: %s\n","output",
                                 strlen(p->outfile) ? p->outfile : "stdout");
    fprintf(p->out,"%9.9s: %s\n","separator", p->separator);
    fprintf(p->out,"%9.9s: ","width");
    for (i=0;i<(int)ArraySize(p->colWidth) && p->colWidth[i] != 0;i++) {
        fprintf(p->out,"%d ",p->colWidth[i]);
    }
    fprintf(p->out,"\n\n");
  }else

  if( c=='t' && n>1 && strncmp(azArg[0], "tables", n)==0 ){
    char **azResult;
    int nRow, rc;
    char *zErrMsg;
    if( nArg==1 ){
      rc = sqlite_get_table(db,
        "SELECT name FROM sqlite_master "
        "WHERE type IN ('table','view') "
        "ORDER BY name",
        &azResult, &nRow, 0, &zErrMsg
      );
    }else{
      rc = sqlite_get_table_printf(db,
        "SELECT name FROM sqlite_master "
        "WHERE type IN ('table','view') AND name LIKE '%%%q%%' "
        "ORDER BY name",
        &azResult, &nRow, 0, &zErrMsg, azArg[1]
      );
    }
    if( zErrMsg ){
      fprintf(stderr,"Error: %s\n", zErrMsg);
      free(zErrMsg);
    }
    if( rc==SQLITE_OK ){
      int len, maxlen = 0;
      int i, j;
      int nPrintCol, nPrintRow;
      for(i=1; i<=nRow; i++){
        if( azResult[i]==0 ) continue;
        len = strlen(azResult[i]);
        if( len>maxlen ) maxlen = len;
      }
      nPrintCol = 80/(maxlen+2);
      if( nPrintCol<1 ) nPrintCol = 1;
      nPrintRow = (nRow + nPrintCol - 1)/nPrintCol;
      for(i=0; i<nPrintRow; i++){
        for(j=i+1; j<=nRow; j+=nPrintRow){
          char *zSp = j<=nPrintRow ? "" : "  ";
          printf("%s%-*s", zSp, maxlen, azResult[j] ? azResult[j] : "");
        }
        printf("\n");
      }
    }
    sqlite_free_table(azResult);
  }else

  if( c=='t' && n>1 && strncmp(azArg[0], "timeout", n)==0 && nArg>=2 ){
    sqlite_busy_timeout(db, atoi(azArg[1]));
  }else

  if( c=='w' && strncmp(azArg[0], "width", n)==0 ){
    int j;
    for(j=1; j<nArg && j<ArraySize(p->colWidth); j++){
      p->colWidth[j-1] = atoi(azArg[j]);
    }
  }else

  {
    fprintf(stderr, "unknown command or invalid arguments: "
      " \"%s\". Enter \".help\" for help\n", azArg[0]);
  }

  return rc;
}

/*
** Read input from *in and process it.  If *in==0 then input
** is interactive - the user is typing it it.  Otherwise, input
** is coming from a file or device.  A prompt is issued and history
** is saved only if input is interactive.  An interrupt signal will
** cause this routine to exit immediately, unless input is interactive.
*/
static void process_input(struct callback_data *p, FILE *in){
  char *zLine;
  char *zSql = 0;
  int nSql = 0;
  char *zErrMsg;
  while( fflush(p->out), (zLine = one_input_line(zSql, in))!=0 ){
    if( seenInterrupt ){
      if( in!=0 ) break;
      seenInterrupt = 0;
    }
    if( p->echoOn ) printf("%s\n", zLine);
    if( zLine && zLine[0]=='.' && nSql==0 ){
      int rc = do_meta_command(zLine, db, p);
      free(zLine);
      if( rc ) break;
      continue;
    }
    if( zSql==0 ){
      int i;
      for(i=0; zLine[i] && isspace(zLine[i]); i++){}
      if( zLine[i]!=0 ){
        nSql = strlen(zLine);
        zSql = malloc( nSql+1 );
        strcpy(zSql, zLine);
      }
    }else{
      int len = strlen(zLine);
      zSql = realloc( zSql, nSql + len + 2 );
      if( zSql==0 ){
        fprintf(stderr,"%s: out of memory!\n", Argv0);
        exit(1);
      }
      strcpy(&zSql[nSql++], "\n");
      strcpy(&zSql[nSql], zLine);
      nSql += len;
    }
    free(zLine);
    if( zSql && sqlite_complete(zSql) ){
      p->cnt = 0;
      if( sqlite_exec(db, zSql, callback, p, &zErrMsg)!=0
           && zErrMsg!=0 ){
        if( in!=0 && !p->echoOn ) printf("%s\n",zSql);
        printf("SQL error: %s\n", zErrMsg);
        free(zErrMsg);
        zErrMsg = 0;
      }
      free(zSql);
      zSql = 0;
      nSql = 0;
    }
  }
  if( zSql ){
    printf("Incomplete SQL: %s\n", zSql);
    free(zSql);
  }
}

/*
** Return a pathname which is the user's home directory.  A
** 0 return indicates an error of some kind.  Space to hold the
** resulting string is obtained from malloc().  The calling
** function should free the result.
*/
static char *find_home_dir(void){
  char *home_dir = NULL;

#if !defined(_WIN32) && !defined(WIN32)
  struct passwd *pwent;
  uid_t uid = getuid();
  while( (pwent=getpwent()) != NULL) {
    if(pwent->pw_uid == uid) {
      home_dir = pwent->pw_dir;
      break;
    }
  }
#endif

  if (!home_dir) {
    home_dir = getenv("HOME");
    if (!home_dir) {
      home_dir = getenv("HOMEPATH"); /* Windows? */
    }
  }

  if( home_dir ){
    char *z = malloc( strlen(home_dir)+1 );
    if( z ) strcpy(z, home_dir);
    home_dir = z;
  }
  return home_dir;
}

/*
** Read input from the file given by sqliterc_override.  Or if that
** parameter is NULL, take input from ~/.sqliterc
*/
static void process_sqliterc(struct callback_data *p, char *sqliterc_override){
  char *home_dir = NULL;
  char *sqliterc = sqliterc_override;
  FILE *in = NULL;

  if (sqliterc == NULL) {
    home_dir = find_home_dir();
    sqliterc = home_dir ? malloc(strlen(home_dir) + 15) : 0;
    if( sqliterc==0 ){
      fprintf(stderr,"%s: out of memory!\n", Argv0);
      exit(1);
    }
    sprintf(sqliterc,"%s/.sqliterc",home_dir);
    free(home_dir);
  }
  in = fopen(sqliterc,"r");
  if(in) {
    printf("Loading resources from %s\n",sqliterc);
    process_input(p,in);
    fclose(in);
  }
  return;
}

/*
** Initialize the state information in data
*/
void main_init(struct callback_data *data) {
  memset(data, 0, sizeof(*data));
  data->mode = MODE_List;
  strcpy(data->separator,"|");
  data->showHeader = 0;
  strcpy(mainPrompt,"sqlite> ");
  strcpy(continuePrompt,"   ...> ");
}

int main(int argc, char **argv){
  char *zErrMsg = 0;
  struct callback_data data;
  int origArgc = argc;
  char **origArgv = argv;

  Argv0 = argv[0];
  main_init(&data);
  process_sqliterc(&data,NULL);

#ifdef SIGINT
  signal(SIGINT, interrupt_handler);
#endif
  while( argc>=2 && argv[1][0]=='-' ){
    if( argc>=3 && strcmp(argv[1],"-init")==0 ){
      /* If we get a -init to do, we have to pretend that
      ** it replaced the .sqliterc file. Soooo, in order to
      ** do that we need to start from scratch...*/
      main_init(&data);

      /* treat this file as the sqliterc... */
      process_sqliterc(&data,argv[2]);

      /* fix up the command line so we do not re-read
      ** the option next time around... */
      {
        int i = 1;
        for(i=1;i<=argc-2;i++) {
          argv[i] = argv[i+2];
        }
      }
      origArgc-=2;

      /* and reset the command line options to be re-read.*/
      argv = origArgv;
      argc = origArgc;

    }else if( strcmp(argv[1],"-html")==0 ){
      data.mode = MODE_Html;
      argc--;
      argv++;
    }else if( strcmp(argv[1],"-list")==0 ){
      data.mode = MODE_List;
      argc--;
      argv++;
    }else if( strcmp(argv[1],"-line")==0 ){
      data.mode = MODE_Line;
      argc--;
      argv++;
    }else if( strcmp(argv[1],"-column")==0 ){
      data.mode = MODE_Column;
      argc--;
      argv++;
    }else if( argc>=3 && strcmp(argv[1],"-separator")==0 ){
      sprintf(data.separator,"%.*s",(int)sizeof(data.separator)-1,argv[2]);
      argc -= 2;
      argv += 2;
    }else if( argc>=3 && strcmp(argv[1],"-nullvalue")==0 ){
      sprintf(data.nullvalue,"%.*s",(int)sizeof(data.nullvalue)-1,argv[2]);
      argc -= 2;
      argv += 2;
    }else if( strcmp(argv[1],"-header")==0 ){
      data.showHeader = 1;
      argc--;
      argv++;
    }else if( strcmp(argv[1],"-noheader")==0 ){
      data.showHeader = 0;
      argc--;
      argv++;
    }else if( strcmp(argv[1],"-echo")==0 ){
      data.echoOn = 1;
      argc--;
      argv++;
    }else{
      fprintf(stderr,"%s: unknown option: %s\n", Argv0, argv[1]);
      return 1;
    }
  }
  if( argc!=2 && argc!=3 ){
    fprintf(stderr,"Usage: %s ?OPTIONS? FILENAME ?SQL?\n", Argv0);
    exit(1);
  }
  data.db = db = sqlite_open(argv[1], 0666, &zErrMsg);
  if( db==0 ){
    data.db = db = sqlite_open(argv[1], 0444, &zErrMsg);
    if( db==0 ){
      if( zErrMsg ){
        fprintf(stderr,"Unable to open database \"%s\": %s\n", argv[1],zErrMsg);
      }else{
        fprintf(stderr,"Unable to open database %s\n", argv[1]);
      }
      exit(1);
    }else{
      fprintf(stderr,"Database \"%s\" opened READ ONLY!\n", argv[1]);
    }
  }
  data.out = stdout;
  if( argc==3 ){
    if( argv[2][0]=='.' ){
      do_meta_command(argv[2], db, &data);
      exit(0);
    }else{
      int rc;
      rc = sqlite_exec(db, argv[2], callback, &data, &zErrMsg);
      if( rc!=0 && zErrMsg!=0 ){
        fprintf(stderr,"SQL error: %s\n", zErrMsg);
        exit(1);
      }
    }
  }else{
    extern int isatty();
    if( isatty(0) ){
      char *zHome;
      char *zHistory = 0;
      printf(
        "SQLite version %s\n"
        "Enter \".help\" for instructions\n",
        sqlite_version
      );
      zHome = find_home_dir();
      if( zHome && (zHistory = malloc(strlen(zHome)+20))!=0 ){
        sprintf(zHistory,"%s/.sqlite_history", zHome);
      }
      if( zHistory ) read_history(zHistory);
      process_input(&data, 0);
      if( zHistory ){
        stifle_history(100);
        write_history(zHistory);
      }
    }else{
      process_input(&data, stdin);
    }
  }
  set_table_name(&data, 0);
  sqlite_close(db);
  return 0;
}
