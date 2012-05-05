/*
** 2012 April 10
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This module implements a VIRTUAL TABLE that can be used to search
** a large vocabulary for close matches.  For example, this virtual
** table can be used to suggest corrections to misspelled words.  Or,
** it could be used with FTS4 to do full-text search using potentially
** misspelled words.
**
** Create an instance of the virtual table this way:
**
**    CREATE VIRTUAL TABLE demo USING spellfix1;
**
** The "spellfix1" term is the name of this module.  The "demo" is the
** name of the virtual table you will be creating.  The table is initially
** empty.  You have to populate it with your vocabulary.  Suppose you
** have a list of words in a table named "big_vocabulary".  Then do this:
**
**    INSERT INTO demo(word) SELECT word FROM big_vocabulary;
**
** If you intend to use this virtual table in cooperation with an FTS4
** table (for spelling correctly of search terms) then you can extract
** the vocabulary using an fts3aux table:
**
**    INSERT INTO demo(word) SELECT term FROM search_aux WHERE col='*';
**
** You can also provide the virtual table with a "rank" for each word.
** The "rank" is an estimate of how common the word is.  Larger numbers
** mean the word is more common.  If you omit the rank when populating
** the table, then a rank of 1 is assumed.  But if you have rank 
** information, you can supply it and the virtual table will show a
** slight preference for selecting more commonly used terms.  To
** populate the rank from an fts4aux table "search_aux" do something
** like this:
**
**    INSERT INTO demo(word,rank)
**        SELECT term, documents FROM search_aux WHERE col='*';
**
** To query the virtual table, include a MATCH operator in the WHERE
** clause.  For example:
**
**    SELECT word FROM demo WHERE word MATCH 'kennasaw';
**
** Using a dataset of American place names (derived from
** http://geonames.usgs.gov/domestic/download_data.htm) the query above
** returns 20 results beginning with:
**
**    kennesaw
**    kenosha
**    kenesaw
**    kenaga
**    keanak
**
** If you append the character '*' to the end of the pattern, then
** a prefix search is performed.  For example:
**
**    SELECT word FROM demo WHERE word MATCH 'kennes*';
**
** Yields 20 results beginning with:
**
**    kennesaw
**    kennestone
**    kenneson
**    kenneys
**    keanes
**    keenes
**
** The virtual table actually has a unique rowid with five columns plus three
** extra hidden columns.  The columns are as follows:
**
**    rowid         A unique integer number associated with each
**                  vocabulary item in the table.  This can be used
**                  as a foreign key on other tables in the database.
**
**    word          The text of the word that matches the pattern.
**                  Both word and pattern can contains unicode characters
**                  and can be mixed case.
**
**    rank          This is the rank of the word, as specified in the
**                  original INSERT statement.
**
**    distance      This is an edit distance or Levensthein distance going
**                  from the pattern to the word.
**
**    langid        This is the language-id of the word.  All queries are
**                  against a single language-id, which defaults to 0.
**                  For any given query this value is the same on all rows.
**
**    score         The score is a combination of rank and distance.  The
**                  idea is that a lower score is better.  The virtual table
**                  attempts to find words with the lowest score and 
**                  by default (unless overridden by ORDER BY) returns
**                  results in order of increasing score.
**
**    top           (HIDDEN)  For any query, this value is the same on all
**                  rows.  It is an integer which is the maximum number of
**                  rows that will be output.  The actually number of rows
**                  output might be less than this number, but it will never
**                  be greater.  The default value for top is 20, but that
**                  can be changed for each query by including a term of
**                  the form "top=N" in the WHERE clause of the query.
**
**    scope         (HIDDEN)  For any query, this value is the same on all
**                  rows.  The scope is a measure of how widely the virtual
**                  table looks for matching words.  Smaller values of
**                  scope cause a broader search.  The scope is normally
**                  choosen automatically and is capped at 4.  Applications
**                  can change the scope by including a term of the form
**                  "scope=N" in the WHERE clause of the query.  Increasing
**                  the scope will make the query run faster, but will reduce
**                  the possible corrections.
**
**    srchcnt       (HIDDEN)  For any query, this value is the same on all
**                  rows.  This value is an integer which is the number of
**                  of words examined using the edit-distance algorithm to
**                  find the top matches that are ultimately displayed.  This
**                  value is for diagnostic use only.
**
**    soundslike    (HIDDEN)  When inserting vocabulary entries, this field
**                  can be set to an spelling that matches what the word
**                  sounds like.  See the DEALING WITH UNUSUAL AND DIFFICULT
**                  SPELLINGS section below for details.
**
** When inserting into or updating the virtual table, only the rowid, word,
** rank, and langid may be changes.  Any attempt to set or modify the values
** of distance, score, top, scope, or srchcnt is silently ignored.
**
** ALGORITHM
**
** A shadow table named "%_vocab" (where the % is replaced by the name of
** the virtual table; Ex: "demo_vocab" for the "demo" virtual table) is
** constructed with these columns:
**
**    id            The unique id (INTEGER PRIMARY KEY)
**
**    rank          The rank of word.
**
**    langid        The language id for this entry.
**
**    word          The original UTF8 text of the vocabulary word
**
**    k1            The word transliterated into lower-case ASCII.  
**                  There is a standard table of mappings from non-ASCII
**                  characters into ASCII.  Examples: "æ" -> "ae",
**                  "þ" -> "th", "ß" -> "ss", "á" -> "a", ...  The
**                  accessory function spellfix1_translit(X) will do
**                  the non-ASCII to ASCII mapping.  The built-in lower(X)
**                  function will convert to lower-case.  Thus:
**                  k1 = lower(spellfix1_translit(word)).
**
**    k2            This field holds a phonetic code derived from k1.  Letters
**                  that have similar sounds are mapped into the same symbol.
**                  For example, all vowels and vowel clusters become the
**                  single symbol "A".  And the letters "p", "b", "f", and
**                  "v" all become "B".  All nasal sounds are represented
**                  as "N".  And so forth.  The mapping is base on
**                  ideas found in Soundex, Metaphone, and other
**                  long-standing phonetic matching systems.  This key can
**                  be generated by the function spellfix1_charclass(X).  
**                  Hence: k2 = spellfix1_charclass(k1)
**
** There is also a function for computing the Wagner edit distance or the
** Levenshtein distance between a pattern and a word.  This function
** is exposed as spellfix1_editdist(X,Y).  The edit distance function
** returns the "cost" of converting X into Y.  Some transformations
** cost more than others.  Changing one vowel into a different vowel,
** for example is relatively cheap, as is doubling a constant, or
** omitting the second character of a double-constant.  Other transformations
** or more expensive.  The idea is that the edit distance function returns
** a low cost of words that are similar and a higher cost for words
** that are futher apart.  In this implementation, the maximum cost
** of any single-character edit (delete, insert, or substitute) is 100,
** with lower costs for some edits (such as transforming vowels).
**
** The "score" for a comparison is the edit distance between the pattern
** and the word, adjusted down by the base-2 logorithm of the word rank.
** For example, a match with distance 100 but rank 1000 would have a
** score of 122 (= 100 - log2(1000) + 32) where as a match with distance
** 100 with a rank of 1 would have a score of 131 (100 - log2(1) + 32).
** (NB:  The constant 32 is added to each score to keep it from going
** negative in case the edit distance is zero.)  In this way, frequently
** used words get a slightly lower cost which tends to move them toward
** the top of the list of alternative spellings.
**
** A straightforward implementation of a spelling corrector would be
** to compare the search term against every word in the vocabulary
** and select the 20 with the lowest scores.  However, there will 
** typically be hundreds of thousands or millions of words in the
** vocabulary, and so this approach is not fast enough.
**
** Suppose the term that is being spell-corrected is X.  To limit
** the search space, X is converted to a k2-like key using the
** equivalent of:
**
**    key = spellfix1_charclass(lower(spellfix1_translit(X)))
**
** This key is then limited to "scope" characters.  The default scope
** value is 4, but an alternative scope can be specified using the
** "scope=N" term in the WHERE clause.  After the key has been truncated,
** the edit distance is run against every term in the vocabulary that
** has a k2 value that begins with the abbreviated key.
**
** For example, suppose the input word is "Paskagula".  The phonetic 
** key is "BACACALA" which is then truncated to 4 characters "BACA".
** The edit distance is then run on the 4980 entries (out of
** 272,597 entries total) of the vocabulary whose k2 values begin with
** BACA, yielding "Pascagoula" as the best match.
** 
** Only terms of the vocabulary with a matching langid are searched.
** Hence, the same table can contain entries from multiple languages
** and only the requested language will be used.  The default langid
** is 0.
**
** DEALING WITH UNUSUAL AND DIFFICULT SPELLINGS
**
** The algorithm above works quite well for most cases, but there are
** exceptions.  These exceptions can be dealt with by making additional
** entries in the virtual table using the "soundslike" column.
**
** For example, many words of Greek origin begin with letters "ps" where
** the "p" is silent.  Ex:  psalm, pseudonym, psoriasis, psyche.  In
** another example, many Scottish surnames can be spelled with an
** initial "Mac" or "Mc".  Thus, "MacKay" and "McKay" are both pronounced
** the same.
**
** Accommodation can be made for words that are not spelled as they
** sound by making additional entries into the virtual table for the
** same word, but adding an alternative spelling in the "soundslike"
** column.  For example, the canonical entry for "psalm" would be this:
**
**   INSERT INTO demo(word) VALUES('psalm');
**
** To enhance the ability to correct the spelling of "salm" into
** "psalm", make an addition entry like this:
**
**   INSERT INTO demo(word,soundslike) VALUES('psalm','salm');
**
** It is ok to make multiple entries for the same word as long as
** each entry has a different soundslike value.  Note that if no
** soundslike value is specified, the soundslike defaults to the word
** itself.
**
** Listed below are some cases where it might make sense to add additional
** soundslike entries.  The specific entries will depend on the application
** and the target language.
**
**   *   Silent "p" in words beginning with "ps":  psalm, psyche
**
**   *   Silent "p" in words beginning with "pn":  pneumonia, pneumatic
**
**   *   Silent "p" in words beginning with "pt":  pterodactyl, ptolemaic
**
**   *   Silent "d" in words beginning with "dj":  djinn, Djikarta
**
**   *   Silent "k" in words beginning with "kn":  knight, Knuthson
**
**   *   Silent "g" in words beginning with "gn":  gnarly, gnome, gnat
**
**   *   "Mac" versus "Mc" beginning Scottish surnames
**
**   *   "Tch" sounds in Slavic words:  Tchaikovsky vs. Chaykovsky
**
**   *   The letter "j" pronounced like "h" in Spanish:  LaJolla
**
**   *   Words beginning with "wr" versus "r":  write vs. rite
**
**   *   Miscellanous problem words such as "debt", "tsetse",
**       "Nguyen", "Van Nuyes".
*/
#if SQLITE_CORE
# include "sqliteInt.h"
#else
# include <string.h>
# include <stdio.h>
# include <stdlib.h>
# include "sqlite3ext.h"
  SQLITE_EXTENSION_INIT1
#endif /* !SQLITE_CORE */

/*
** Character classes for ASCII characters:
**
**   0   ''        Silent letters:   H W
**   1   'A'       Any vowel:   A E I O U (Y)
**   2   'B'       A bilabeal stop or fricative:  B F P V
**   3   'C'       Other fricatives or back stops:  C G J K Q S X Z
**   4   'D'       Alveolar stops:  D T
**   5   'H'       Letter H at the beginning of a word
**   6   'L'       Glides:  L R
**   7   'M'       Nasals:  M N
**   8   'W'       Letter W at the beginning of a word
**   9   'Y'       Letter Y at the beginning of a word.
**   10  '9'       A digit: 0 1 2 3 4 5 6 7 8 9
**   11  ' '       White space
**   12  '?'       Other.
*/
#define CCLASS_SILENT         0
#define CCLASS_VOWEL          1
#define CCLASS_B              2
#define CCLASS_C              3
#define CCLASS_D              4
#define CCLASS_H              5
#define CCLASS_L              6
#define CCLASS_M              7
#define CCLASS_W              8
#define CCLASS_Y              9
#define CCLASS_DIGIT         10
#define CCLASS_SPACE         11
#define CCLASS_OTHER         12

/*
** The following table gives the character class for non-initial ASCII
** characters.
*/
static const unsigned char midClass[] = {
          /* x0  x1  x2  x3  x4  x5  x6  x7    x8  x9  xa  xb  xc  xd  xe  xf */
  /* 0x */   12, 12, 12, 12, 12, 12, 12, 12,   12, 11, 11, 12, 11, 12, 12, 12,
  /* 1x */   12, 12, 12, 12, 12, 12, 12, 12,   12, 12, 12, 12, 12, 12, 12, 12,
  /* 2x */   11, 12, 12, 12, 12, 12, 12, 12,   12, 12, 12, 12, 12, 12, 12, 12,
  /* 3x */   10, 10, 10, 10, 10, 10, 10, 10,   10, 10, 12, 12, 12, 12, 12, 12,
  /* 4x */   12,  1,  2,  3,  4,  1,  2,  3,    0,  1,  3,  3,  6,  7,  7,  1,
  /* 5x */    2,  3,  6,  3,  4,  1,  2,  0,    3,  1,  3, 12, 12, 12, 12, 12,
  /* 6x */   12,  1,  2,  3,  4,  1,  2,  3,    0,  1,  3,  3,  6,  7,  7,  1,
  /* 7x */    2,  3,  6,  3,  4,  1,  2,  0,    3,  1,  3, 12, 12, 12, 12, 12,
};

/* 
** This tables gives the character class for ASCII characters that form the
** initial character of a word.  The only difference from midClass is with
** the letters H, W, and Y.
*/
static const unsigned char initClass[] = {
          /* x0  x1  x2  x3  x4  x5  x6  x7    x8  x9  xa  xb  xc  xd  xe  xf */
  /* 0x */   12, 12, 12, 12, 12, 12, 12, 12,   12, 11, 11, 12, 11, 12, 12, 12,
  /* 1x */   12, 12, 12, 12, 12, 12, 12, 12,   12, 12, 12, 12, 12, 12, 12, 12,
  /* 2x */   11, 12, 12, 12, 12, 12, 12, 12,   12, 12, 12, 12, 12, 12, 12, 12,
  /* 3x */   10, 10, 10, 10, 10, 10, 10, 10,   10, 10, 12, 12, 12, 12, 12, 12,
  /* 4x */   12,  1,  2,  3,  4,  1,  2,  3,    5,  1,  3,  3,  6,  7,  7,  1,
  /* 5x */    2,  3,  6,  3,  4,  1,  2,  8,    3,  9,  3, 12, 12, 12, 12, 12,
  /* 6x */   12,  1,  2,  3,  4,  1,  2,  3,    5,  1,  3,  3,  6,  7,  7,  1,
  /* 7x */    2,  3,  6,  3,  4,  1,  2,  8,    3,  9,  3, 12, 12, 12, 12, 12,
};

/*
** Mapping from the character class number (0-12) to a symbol for each
** character class.  Note that initClass[] can be used to map the class
** symbol back into the class number.
*/
static const unsigned char className[] = ".ABCDHLMWY9 ?";

/*
** Generate a string of character classes corresponding to the
** ASCII characters in the input string zIn.  If the input is not
** ASCII then the behavior is undefined.
**
** Space to hold the result is obtained from sqlite3_malloc()
**
** Return NULL if memory allocation fails.  
*/
static unsigned char *characterClassString(const unsigned char *zIn, int nIn){
  unsigned char *zOut = sqlite3_malloc( nIn + 1 );
  int i;
  int nOut = 0;
  char cPrev = 0x77;
  const unsigned char *aClass = initClass;

  if( zOut==0 ) return 0;
  for(i=0; i<nIn; i++){
    unsigned char c = zIn[i];
    c = aClass[c&0x7f];
    if( c==CCLASS_OTHER && cPrev!=CCLASS_DIGIT ) continue;
    cPrev = c;
    if( c==CCLASS_SILENT ) continue;
    if( c==CCLASS_SPACE ) continue;
    aClass = midClass;
    c = className[c];
    if( c!=zOut[nOut-1] ) zOut[nOut++] = c;
  }
  zOut[nOut] = 0;
  return zOut;
}

/*
** This is an SQL function wrapper around characterClassString().  See
** the description of characterClassString() for additional information.
*/
static void characterClassSqlFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zIn;
  unsigned char *zOut;

  zIn = sqlite3_value_text(argv[0]);
  if( zIn==0 ) return;
  zOut = characterClassString(zIn, sqlite3_value_bytes(argv[0]));
  if( zOut==0 ){
    sqlite3_result_error_nomem(context);
  }else{
    sqlite3_result_text(context, (char*)zOut, -1, sqlite3_free);
  }
}

/*
** Return the character class number for a character given its
** context.
*/
static char characterClass(char cPrev, char c){
  return cPrev==0 ? initClass[c&0x7f] : midClass[c&0x7f];
}

/*
** Return the cost of inserting or deleting character c immediately
** following character cPrev.  If cPrev==0, that means c is the first
** character of the word.
*/
static int insertOrDeleteCost(char cPrev, char c){
  char classC = characterClass(cPrev, c);
  char classCprev;

  if( classC==CCLASS_SILENT ){
    /* Insert or delete "silent" characters such as H or W */
    return 1;
  }
  if( cPrev==c ){
    /* Repeated characters, or miss a repeat */
    return 10;
  }
  classCprev = characterClass(cPrev, cPrev);
  if( classC==classCprev ){
    if( classC==CCLASS_VOWEL ){
      /* Remove or add a new vowel to a vowel cluster */
      return 15;
    }else{
      /* Remove or add a consonant not in the same class */
      return 50;
    }
  }

  /* any other character insertion or deletion */
  return 100;
}

/*
** Divide the insertion cost by this factor when appending to the
** end of the word.
*/
#define FINAL_INS_COST_DIV  4

/*
** Return the cost of substituting cTo in place of cFrom assuming
** the previous character is cPrev.  If cPrev==0 then cTo is the first
** character of the word.
*/
static int substituteCost(char cPrev, char cFrom, char cTo){
  char classFrom, classTo;
  if( cFrom==cTo ){
    /* Exact match */
    return 0;
  }
  if( cFrom==(cTo^0x20) && ((cTo>='A' && cTo<='Z') || (cTo>='a' && cTo<='z')) ){
    /* differ only in case */
    return 0;
  }
  classFrom = characterClass(cPrev, cFrom);
  classTo = characterClass(cPrev, cTo);
  if( classFrom==classTo ){
    /* Same character class */
    return classFrom=='A' ? 25 : 40;
  }
  if( classFrom>=CCLASS_B && classFrom<=CCLASS_Y
      && classTo>=CCLASS_B && classTo<=CCLASS_Y ){
    /* Convert from one consonant to another, but in a different class */
    return 75;
  }
  /* Any other subsitution */
  return 100;
}

/*
** Given two strings zA and zB which are pure ASCII, return the cost
** of transforming zA into zB.  If zA ends with '*' assume that it is
** a prefix of zB and give only minimal penalty for extra characters
** on the end of zB.
**
** Smaller numbers mean a closer match.
**
** Negative values indicate an error:
**    -1  One of the inputs is NULL
**    -2  Non-ASCII characters on input
**    -3  Unable to allocate memory 
*/
static int editdist(const char *zA, const char *zB){
  int nA, nB;            /* Number of characters in zA[] and zB[] */
  int xA, xB;            /* Loop counters for zA[] and zB[] */
  char cA, cB;           /* Current character of zA and zB */
  char cAprev, cBprev;   /* Previous character of zA and zB */
  int d;                 /* North-west cost value */
  int dc = 0;            /* North-west character value */
  int res;               /* Final result */
  int *m;                /* The cost matrix */
  char *cx;              /* Corresponding character values */
  int *toFree = 0;       /* Malloced space */
  int mStack[60+15];     /* Stack space to use if not too much is needed */

  /* Early out if either input is NULL */
  if( zA==0 || zB==0 ) return -1;

  /* Skip any common prefix */
  while( zA[0] && zA[0]==zB[0] ){ dc = zA[0]; zA++; zB++; }
  if( zA[0]==0 && zB[0]==0 ) return 0;

#if 0
  printf("A=\"%s\" B=\"%s\" dc=%c\n", zA, zB, dc?dc:' ');
#endif

  /* Verify input strings and measure their lengths */
  for(nA=0; zA[nA]; nA++){
    if( zA[nA]>127 ) return -2;
  }
  for(nB=0; zB[nB]; nB++){
    if( zB[nB]>127 ) return -2;
  }

  /* Special processing if either string is empty */
  if( nA==0 ){
    cBprev = dc;
    for(xB=res=0; (cB = zB[xB])!=0; xB++){
      res += insertOrDeleteCost(cBprev, cB)/FINAL_INS_COST_DIV;
      cBprev = cB;
    }
    return res;
  }
  if( nB==0 ){
    cAprev = dc;
    for(xA=res=0; (cA = zA[xA])!=0; xA++){
      res += insertOrDeleteCost(cAprev, cA);
      cAprev = cA;
    }
    return res;
  }

  /* A is a prefix of B */
  if( zA[0]=='*' && zA[1]==0 ) return 0;

  /* Allocate and initialize the Wagner matrix */
  if( nB<(sizeof(mStack)*4)/(sizeof(mStack[0])*5) ){
    m = mStack;
  }else{
    m = toFree = sqlite3_malloc( (nB+1)*5*sizeof(m[0])/4 );
    if( m==0 ) return -3;
  }
  cx = (char*)&m[nB+1];

  /* Compute the Wagner edit distance */
  m[0] = 0;
  cx[0] = dc;
  cBprev = dc;
  for(xB=1; xB<=nB; xB++){
    cB = zB[xB-1];
    cx[xB] = cB;
    m[xB] = m[xB-1] + insertOrDeleteCost(cBprev, cB);
    cBprev = cB;
  }
  cAprev = dc;
  for(xA=1; xA<=nA; xA++){
    int lastA = (xA==nA);
    cA = zA[xA-1];
    if( cA=='*' && lastA ) break;
    d = m[0];
    dc = cx[0];
    m[0] = d + insertOrDeleteCost(cAprev, cA);
    cBprev = 0;
    for(xB=1; xB<=nB; xB++){
      int totalCost, insCost, delCost, subCost, ncx;
      cB = zB[xB-1];

      /* Cost to insert cB */
      insCost = insertOrDeleteCost(cx[xB-1], cB);
      if( lastA ) insCost /= FINAL_INS_COST_DIV;

      /* Cost to delete cA */
      delCost = insertOrDeleteCost(cx[xB], cA);

      /* Cost to substitute cA->cB */
      subCost = substituteCost(cx[xB-1], cA, cB);

      /* Best cost */
      totalCost = insCost + m[xB-1];
      ncx = cB;
      if( (delCost + m[xB])<totalCost ){
        totalCost = delCost + m[xB];
        ncx = cA;
      }
      if( (subCost + d)<totalCost ){
        totalCost = subCost + d;
      }

#if 0
      printf("%d,%d d=%4d u=%4d r=%4d dc=%c cA=%c cB=%c"
             " ins=%4d del=%4d sub=%4d t=%4d ncx=%c\n",
             xA, xB, d, m[xB], m[xB-1], dc?dc:' ', cA, cB,
             insCost, delCost, subCost, totalCost, ncx?ncx:' ');
#endif

      /* Update the matrix */
      d = m[xB];
      dc = cx[xB];
      m[xB] = totalCost;
      cx[xB] = ncx;
      cBprev = cB;
    }
    cAprev = cA;
  }

  /* Free the wagner matrix and return the result */
  if( cA=='*' && nB>nA ){
    res = m[nA];
    for(xB=nA+1; xB<=nB; xB++){
      if( m[xB]<res ) res = m[xB];
    }
  }else{
    res = m[nB];
  }
  sqlite3_free(toFree);
  return res;
}

/*
** Function:    editdist(A,B)
**
** Return the cost of transforming string A into string B.  Both strings
** must be pure ASCII text.  If A ends with '*' then it is assumed to be
** a prefix of B and extra characters on the end of B have minimal additional
** cost.
*/
static void editdistSqlFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int res = editdist((const char*)sqlite3_value_text(argv[0]),
                    (const char*)sqlite3_value_text(argv[1]));
  if( res<0 ){
    if( res==(-3) ){
      sqlite3_result_error_nomem(context);
    }else if( res==(-2) ){
      sqlite3_result_error(context, "non-ASCII input to editdist()", -1);
    }else{
      sqlite3_result_error(context, "NULL input to editdist()", -1);
    }
  }else{ 
    sqlite3_result_int(context, res);
  }
}

#if !SQLITE_CORE
/*
** This lookup table is used to help decode the first byte of
** a multi-byte UTF8 character.
*/
static const unsigned char sqlite3Utf8Trans1[] = {
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
  0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x00, 0x00,
};
#endif

/*
** Return the value of the first UTF-8 character in the string.
*/
static int utf8Read(const unsigned char *z, int n, int *pSize){
  int c, i;

  if( n==0 ){
    c = i = 0;
  }else{
    c = z[0];
    i = 1;
    if( c>=0xc0 ){
      c = sqlite3Utf8Trans1[c-0xc0];
      while( i<n && (z[i] & 0xc0)==0x80 ){
        c = (c<<6) + (0x3f & z[i++]);
      }
    }
  }
  *pSize = i;
  return c;
}

/*
** Table of translations from unicode characters into ASCII.
*/
static const struct {
 unsigned short int cFrom;
 unsigned char cTo0, cTo1;
} translit[] = {
  { 0x00A0,  0x20, 0x00 },  /*   to   */
  { 0x00B5,  0x75, 0x00 },  /* µ to u */
  { 0x00C0,  0x41, 0x00 },  /* À to A */
  { 0x00C1,  0x41, 0x00 },  /* Á to A */
  { 0x00C2,  0x41, 0x00 },  /* Â to A */
  { 0x00C3,  0x41, 0x00 },  /* Ã to A */
  { 0x00C4,  0x41, 0x65 },  /* Ä to Ae */
  { 0x00C5,  0x41, 0x61 },  /* Å to Aa */
  { 0x00C6,  0x41, 0x45 },  /* Æ to AE */
  { 0x00C7,  0x43, 0x00 },  /* Ç to C */
  { 0x00C8,  0x45, 0x00 },  /* È to E */
  { 0x00C9,  0x45, 0x00 },  /* É to E */
  { 0x00CA,  0x45, 0x00 },  /* Ê to E */
  { 0x00CB,  0x45, 0x00 },  /* Ë to E */
  { 0x00CC,  0x49, 0x00 },  /* Ì to I */
  { 0x00CD,  0x49, 0x00 },  /* Í to I */
  { 0x00CE,  0x49, 0x00 },  /* Î to I */
  { 0x00CF,  0x49, 0x00 },  /* Ï to I */
  { 0x00D0,  0x44, 0x00 },  /* Ð to D */
  { 0x00D1,  0x4E, 0x00 },  /* Ñ to N */
  { 0x00D2,  0x4F, 0x00 },  /* Ò to O */
  { 0x00D3,  0x4F, 0x00 },  /* Ó to O */
  { 0x00D4,  0x4F, 0x00 },  /* Ô to O */
  { 0x00D5,  0x4F, 0x00 },  /* Õ to O */
  { 0x00D6,  0x4F, 0x65 },  /* Ö to Oe */
  { 0x00D7,  0x78, 0x00 },  /* × to x */
  { 0x00D8,  0x4F, 0x00 },  /* Ø to O */
  { 0x00D9,  0x55, 0x00 },  /* Ù to U */
  { 0x00DA,  0x55, 0x00 },  /* Ú to U */
  { 0x00DB,  0x55, 0x00 },  /* Û to U */
  { 0x00DC,  0x55, 0x65 },  /* Ü to Ue */
  { 0x00DD,  0x59, 0x00 },  /* Ý to Y */
  { 0x00DE,  0x54, 0x68 },  /* Þ to Th */
  { 0x00DF,  0x73, 0x73 },  /* ß to ss */
  { 0x00E0,  0x61, 0x00 },  /* à to a */
  { 0x00E1,  0x61, 0x00 },  /* á to a */
  { 0x00E2,  0x61, 0x00 },  /* â to a */
  { 0x00E3,  0x61, 0x00 },  /* ã to a */
  { 0x00E4,  0x61, 0x65 },  /* ä to ae */
  { 0x00E5,  0x61, 0x61 },  /* å to aa */
  { 0x00E6,  0x61, 0x65 },  /* æ to ae */
  { 0x00E7,  0x63, 0x00 },  /* ç to c */
  { 0x00E8,  0x65, 0x00 },  /* è to e */
  { 0x00E9,  0x65, 0x00 },  /* é to e */
  { 0x00EA,  0x65, 0x00 },  /* ê to e */
  { 0x00EB,  0x65, 0x00 },  /* ë to e */
  { 0x00EC,  0x69, 0x00 },  /* ì to i */
  { 0x00ED,  0x69, 0x00 },  /* í to i */
  { 0x00EE,  0x69, 0x00 },  /* î to i */
  { 0x00EF,  0x69, 0x00 },  /* ï to i */
  { 0x00F0,  0x64, 0x00 },  /* ð to d */
  { 0x00F1,  0x6E, 0x00 },  /* ñ to n */
  { 0x00F2,  0x6F, 0x00 },  /* ò to o */
  { 0x00F3,  0x6F, 0x00 },  /* ó to o */
  { 0x00F4,  0x6F, 0x00 },  /* ô to o */
  { 0x00F5,  0x6F, 0x00 },  /* õ to o */
  { 0x00F6,  0x6F, 0x65 },  /* ö to oe */
  { 0x00F7,  0x3A, 0x00 },  /* ÷ to : */
  { 0x00F8,  0x6F, 0x00 },  /* ø to o */
  { 0x00F9,  0x75, 0x00 },  /* ù to u */
  { 0x00FA,  0x75, 0x00 },  /* ú to u */
  { 0x00FB,  0x75, 0x00 },  /* û to u */
  { 0x00FC,  0x75, 0x65 },  /* ü to ue */
  { 0x00FD,  0x79, 0x00 },  /* ý to y */
  { 0x00FE,  0x74, 0x68 },  /* þ to th */
  { 0x00FF,  0x79, 0x00 },  /* ÿ to y */
  { 0x0100,  0x41, 0x00 },  /* Ā to A */
  { 0x0101,  0x61, 0x00 },  /* ā to a */
  { 0x0102,  0x41, 0x00 },  /* Ă to A */
  { 0x0103,  0x61, 0x00 },  /* ă to a */
  { 0x0104,  0x41, 0x00 },  /* Ą to A */
  { 0x0105,  0x61, 0x00 },  /* ą to a */
  { 0x0106,  0x43, 0x00 },  /* Ć to C */
  { 0x0107,  0x63, 0x00 },  /* ć to c */
  { 0x0108,  0x43, 0x68 },  /* Ĉ to Ch */
  { 0x0109,  0x63, 0x68 },  /* ĉ to ch */
  { 0x010A,  0x43, 0x00 },  /* Ċ to C */
  { 0x010B,  0x63, 0x00 },  /* ċ to c */
  { 0x010C,  0x43, 0x00 },  /* Č to C */
  { 0x010D,  0x63, 0x00 },  /* č to c */
  { 0x010E,  0x44, 0x00 },  /* Ď to D */
  { 0x010F,  0x64, 0x00 },  /* ď to d */
  { 0x0110,  0x44, 0x00 },  /* Đ to D */
  { 0x0111,  0x64, 0x00 },  /* đ to d */
  { 0x0112,  0x45, 0x00 },  /* Ē to E */
  { 0x0113,  0x65, 0x00 },  /* ē to e */
  { 0x0114,  0x45, 0x00 },  /* Ĕ to E */
  { 0x0115,  0x65, 0x00 },  /* ĕ to e */
  { 0x0116,  0x45, 0x00 },  /* Ė to E */
  { 0x0117,  0x65, 0x00 },  /* ė to e */
  { 0x0118,  0x45, 0x00 },  /* Ę to E */
  { 0x0119,  0x65, 0x00 },  /* ę to e */
  { 0x011A,  0x45, 0x00 },  /* Ě to E */
  { 0x011B,  0x65, 0x00 },  /* ě to e */
  { 0x011C,  0x47, 0x68 },  /* Ĝ to Gh */
  { 0x011D,  0x67, 0x68 },  /* ĝ to gh */
  { 0x011E,  0x47, 0x00 },  /* Ğ to G */
  { 0x011F,  0x67, 0x00 },  /* ğ to g */
  { 0x0120,  0x47, 0x00 },  /* Ġ to G */
  { 0x0121,  0x67, 0x00 },  /* ġ to g */
  { 0x0122,  0x47, 0x00 },  /* Ģ to G */
  { 0x0123,  0x67, 0x00 },  /* ģ to g */
  { 0x0124,  0x48, 0x68 },  /* Ĥ to Hh */
  { 0x0125,  0x68, 0x68 },  /* ĥ to hh */
  { 0x0126,  0x48, 0x00 },  /* Ħ to H */
  { 0x0127,  0x68, 0x00 },  /* ħ to h */
  { 0x0128,  0x49, 0x00 },  /* Ĩ to I */
  { 0x0129,  0x69, 0x00 },  /* ĩ to i */
  { 0x012A,  0x49, 0x00 },  /* Ī to I */
  { 0x012B,  0x69, 0x00 },  /* ī to i */
  { 0x012C,  0x49, 0x00 },  /* Ĭ to I */
  { 0x012D,  0x69, 0x00 },  /* ĭ to i */
  { 0x012E,  0x49, 0x00 },  /* Į to I */
  { 0x012F,  0x69, 0x00 },  /* į to i */
  { 0x0130,  0x49, 0x00 },  /* İ to I */
  { 0x0131,  0x69, 0x00 },  /* ı to i */
  { 0x0132,  0x49, 0x4A },  /* Ĳ to IJ */
  { 0x0133,  0x69, 0x6A },  /* ĳ to ij */
  { 0x0134,  0x4A, 0x68 },  /* Ĵ to Jh */
  { 0x0135,  0x6A, 0x68 },  /* ĵ to jh */
  { 0x0136,  0x4B, 0x00 },  /* Ķ to K */
  { 0x0137,  0x6B, 0x00 },  /* ķ to k */
  { 0x0138,  0x6B, 0x00 },  /* ĸ to k */
  { 0x0139,  0x4C, 0x00 },  /* Ĺ to L */
  { 0x013A,  0x6C, 0x00 },  /* ĺ to l */
  { 0x013B,  0x4C, 0x00 },  /* Ļ to L */
  { 0x013C,  0x6C, 0x00 },  /* ļ to l */
  { 0x013D,  0x4C, 0x00 },  /* Ľ to L */
  { 0x013E,  0x6C, 0x00 },  /* ľ to l */
  { 0x013F,  0x4C, 0x2E },  /* Ŀ to L. */
  { 0x0140,  0x6C, 0x2E },  /* ŀ to l. */
  { 0x0141,  0x4C, 0x00 },  /* Ł to L */
  { 0x0142,  0x6C, 0x00 },  /* ł to l */
  { 0x0143,  0x4E, 0x00 },  /* Ń to N */
  { 0x0144,  0x6E, 0x00 },  /* ń to n */
  { 0x0145,  0x4E, 0x00 },  /* Ņ to N */
  { 0x0146,  0x6E, 0x00 },  /* ņ to n */
  { 0x0147,  0x4E, 0x00 },  /* Ň to N */
  { 0x0148,  0x6E, 0x00 },  /* ň to n */
  { 0x0149,  0x27, 0x6E },  /* ŉ to 'n */
  { 0x014A,  0x4E, 0x47 },  /* Ŋ to NG */
  { 0x014B,  0x6E, 0x67 },  /* ŋ to ng */
  { 0x014C,  0x4F, 0x00 },  /* Ō to O */
  { 0x014D,  0x6F, 0x00 },  /* ō to o */
  { 0x014E,  0x4F, 0x00 },  /* Ŏ to O */
  { 0x014F,  0x6F, 0x00 },  /* ŏ to o */
  { 0x0150,  0x4F, 0x00 },  /* Ő to O */
  { 0x0151,  0x6F, 0x00 },  /* ő to o */
  { 0x0152,  0x4F, 0x45 },  /* Œ to OE */
  { 0x0153,  0x6F, 0x65 },  /* œ to oe */
  { 0x0154,  0x52, 0x00 },  /* Ŕ to R */
  { 0x0155,  0x72, 0x00 },  /* ŕ to r */
  { 0x0156,  0x52, 0x00 },  /* Ŗ to R */
  { 0x0157,  0x72, 0x00 },  /* ŗ to r */
  { 0x0158,  0x52, 0x00 },  /* Ř to R */
  { 0x0159,  0x72, 0x00 },  /* ř to r */
  { 0x015A,  0x53, 0x00 },  /* Ś to S */
  { 0x015B,  0x73, 0x00 },  /* ś to s */
  { 0x015C,  0x53, 0x68 },  /* Ŝ to Sh */
  { 0x015D,  0x73, 0x68 },  /* ŝ to sh */
  { 0x015E,  0x53, 0x00 },  /* Ş to S */
  { 0x015F,  0x73, 0x00 },  /* ş to s */
  { 0x0160,  0x53, 0x00 },  /* Š to S */
  { 0x0161,  0x73, 0x00 },  /* š to s */
  { 0x0162,  0x54, 0x00 },  /* Ţ to T */
  { 0x0163,  0x74, 0x00 },  /* ţ to t */
  { 0x0164,  0x54, 0x00 },  /* Ť to T */
  { 0x0165,  0x74, 0x00 },  /* ť to t */
  { 0x0166,  0x54, 0x00 },  /* Ŧ to T */
  { 0x0167,  0x74, 0x00 },  /* ŧ to t */
  { 0x0168,  0x55, 0x00 },  /* Ũ to U */
  { 0x0169,  0x75, 0x00 },  /* ũ to u */
  { 0x016A,  0x55, 0x00 },  /* Ū to U */
  { 0x016B,  0x75, 0x00 },  /* ū to u */
  { 0x016C,  0x55, 0x00 },  /* Ŭ to U */
  { 0x016D,  0x75, 0x00 },  /* ŭ to u */
  { 0x016E,  0x55, 0x00 },  /* Ů to U */
  { 0x016F,  0x75, 0x00 },  /* ů to u */
  { 0x0170,  0x55, 0x00 },  /* Ű to U */
  { 0x0171,  0x75, 0x00 },  /* ű to u */
  { 0x0172,  0x55, 0x00 },  /* Ų to U */
  { 0x0173,  0x75, 0x00 },  /* ų to u */
  { 0x0174,  0x57, 0x00 },  /* Ŵ to W */
  { 0x0175,  0x77, 0x00 },  /* ŵ to w */
  { 0x0176,  0x59, 0x00 },  /* Ŷ to Y */
  { 0x0177,  0x79, 0x00 },  /* ŷ to y */
  { 0x0178,  0x59, 0x00 },  /* Ÿ to Y */
  { 0x0179,  0x5A, 0x00 },  /* Ź to Z */
  { 0x017A,  0x7A, 0x00 },  /* ź to z */
  { 0x017B,  0x5A, 0x00 },  /* Ż to Z */
  { 0x017C,  0x7A, 0x00 },  /* ż to z */
  { 0x017D,  0x5A, 0x00 },  /* Ž to Z */
  { 0x017E,  0x7A, 0x00 },  /* ž to z */
  { 0x017F,  0x73, 0x00 },  /* ſ to s */
  { 0x0192,  0x66, 0x00 },  /* ƒ to f */
  { 0x0218,  0x53, 0x00 },  /* Ș to S */
  { 0x0219,  0x73, 0x00 },  /* ș to s */
  { 0x021A,  0x54, 0x00 },  /* Ț to T */
  { 0x021B,  0x74, 0x00 },  /* ț to t */
  { 0x0386,  0x41, 0x00 },  /* Ά to A */
  { 0x0388,  0x45, 0x00 },  /* Έ to E */
  { 0x0389,  0x49, 0x00 },  /* Ή to I */
  { 0x038A,  0x49, 0x00 },  /* Ί to I */
  { 0x038C,  0x4f, 0x00 },  /* Ό to O */
  { 0x038E,  0x59, 0x00 },  /* Ύ to Y */
  { 0x038F,  0x4f, 0x00 },  /* Ώ to O */
  { 0x0390,  0x69, 0x00 },  /* ΐ to i */
  { 0x0391,  0x41, 0x00 },  /* Α to A */
  { 0x0392,  0x42, 0x00 },  /* Β to B */
  { 0x0393,  0x47, 0x00 },  /* Γ to G */
  { 0x0394,  0x44, 0x00 },  /* Δ to D */
  { 0x0395,  0x45, 0x00 },  /* Ε to E */
  { 0x0396,  0x5a, 0x00 },  /* Ζ to Z */
  { 0x0397,  0x49, 0x00 },  /* Η to I */
  { 0x0398,  0x54, 0x68 },  /* Θ to Th */
  { 0x0399,  0x49, 0x00 },  /* Ι to I */
  { 0x039A,  0x4b, 0x00 },  /* Κ to K */
  { 0x039B,  0x4c, 0x00 },  /* Λ to L */
  { 0x039C,  0x4d, 0x00 },  /* Μ to M */
  { 0x039D,  0x4e, 0x00 },  /* Ν to N */
  { 0x039E,  0x58, 0x00 },  /* Ξ to X */
  { 0x039F,  0x4f, 0x00 },  /* Ο to O */
  { 0x03A0,  0x50, 0x00 },  /* Π to P */
  { 0x03A1,  0x52, 0x00 },  /* Ρ to R */
  { 0x03A3,  0x53, 0x00 },  /* Σ to S */
  { 0x03A4,  0x54, 0x00 },  /* Τ to T */
  { 0x03A5,  0x59, 0x00 },  /* Υ to Y */
  { 0x03A6,  0x46, 0x00 },  /* Φ to F */
  { 0x03A7,  0x43, 0x68 },  /* Χ to Ch */
  { 0x03A8,  0x50, 0x73 },  /* Ψ to Ps */
  { 0x03A9,  0x4f, 0x00 },  /* Ω to O */
  { 0x03AA,  0x49, 0x00 },  /* Ϊ to I */
  { 0x03AB,  0x59, 0x00 },  /* Ϋ to Y */
  { 0x03AC,  0x61, 0x00 },  /* ά to a */
  { 0x03AD,  0x65, 0x00 },  /* έ to e */
  { 0x03AE,  0x69, 0x00 },  /* ή to i */
  { 0x03AF,  0x69, 0x00 },  /* ί to i */
  { 0x03B1,  0x61, 0x00 },  /* α to a */
  { 0x03B2,  0x62, 0x00 },  /* β to b */
  { 0x03B3,  0x67, 0x00 },  /* γ to g */
  { 0x03B4,  0x64, 0x00 },  /* δ to d */
  { 0x03B5,  0x65, 0x00 },  /* ε to e */
  { 0x03B6,  0x7a, 0x00 },  /* ζ to z */
  { 0x03B7,  0x69, 0x00 },  /* η to i */
  { 0x03B8,  0x74, 0x68 },  /* θ to th */
  { 0x03B9,  0x69, 0x00 },  /* ι to i */
  { 0x03BA,  0x6b, 0x00 },  /* κ to k */
  { 0x03BB,  0x6c, 0x00 },  /* λ to l */
  { 0x03BC,  0x6d, 0x00 },  /* μ to m */
  { 0x03BD,  0x6e, 0x00 },  /* ν to n */
  { 0x03BE,  0x78, 0x00 },  /* ξ to x */
  { 0x03BF,  0x6f, 0x00 },  /* ο to o */
  { 0x03C0,  0x70, 0x00 },  /* π to p */
  { 0x03C1,  0x72, 0x00 },  /* ρ to r */
  { 0x03C3,  0x73, 0x00 },  /* σ to s */
  { 0x03C4,  0x74, 0x00 },  /* τ to t */
  { 0x03C5,  0x79, 0x00 },  /* υ to y */
  { 0x03C6,  0x66, 0x00 },  /* φ to f */
  { 0x03C7,  0x63, 0x68 },  /* χ to ch */
  { 0x03C8,  0x70, 0x73 },  /* ψ to ps */
  { 0x03C9,  0x6f, 0x00 },  /* ω to o */
  { 0x03CA,  0x69, 0x00 },  /* ϊ to i */
  { 0x03CB,  0x79, 0x00 },  /* ϋ to y */
  { 0x03CC,  0x6f, 0x00 },  /* ό to o */
  { 0x03CD,  0x79, 0x00 },  /* ύ to y */
  { 0x03CE,  0x69, 0x00 },  /* ώ to i */
  { 0x0400,  0x45, 0x00 },  /* Ѐ to E */
  { 0x0401,  0x45, 0x00 },  /* Ё to E */
  { 0x0402,  0x44, 0x00 },  /* Ђ to D */
  { 0x0403,  0x47, 0x00 },  /* Ѓ to G */
  { 0x0404,  0x45, 0x00 },  /* Є to E */
  { 0x0405,  0x5a, 0x00 },  /* Ѕ to Z */
  { 0x0406,  0x49, 0x00 },  /* І to I */
  { 0x0407,  0x49, 0x00 },  /* Ї to I */
  { 0x0408,  0x4a, 0x00 },  /* Ј to J */
  { 0x0409,  0x49, 0x00 },  /* Љ to I */
  { 0x040A,  0x4e, 0x00 },  /* Њ to N */
  { 0x040B,  0x44, 0x00 },  /* Ћ to D */
  { 0x040C,  0x4b, 0x00 },  /* Ќ to K */
  { 0x040D,  0x49, 0x00 },  /* Ѝ to I */
  { 0x040E,  0x55, 0x00 },  /* Ў to U */
  { 0x040F,  0x44, 0x00 },  /* Џ to D */
  { 0x0410,  0x41, 0x00 },  /* А to A */
  { 0x0411,  0x42, 0x00 },  /* Б to B */
  { 0x0412,  0x56, 0x00 },  /* В to V */
  { 0x0413,  0x47, 0x00 },  /* Г to G */
  { 0x0414,  0x44, 0x00 },  /* Д to D */
  { 0x0415,  0x45, 0x00 },  /* Е to E */
  { 0x0416,  0x5a, 0x68 },  /* Ж to Zh */
  { 0x0417,  0x5a, 0x00 },  /* З to Z */
  { 0x0418,  0x49, 0x00 },  /* И to I */
  { 0x0419,  0x49, 0x00 },  /* Й to I */
  { 0x041A,  0x4b, 0x00 },  /* К to K */
  { 0x041B,  0x4c, 0x00 },  /* Л to L */
  { 0x041C,  0x4d, 0x00 },  /* М to M */
  { 0x041D,  0x4e, 0x00 },  /* Н to N */
  { 0x041E,  0x4f, 0x00 },  /* О to O */
  { 0x041F,  0x50, 0x00 },  /* П to P */
  { 0x0420,  0x52, 0x00 },  /* Р to R */
  { 0x0421,  0x53, 0x00 },  /* С to S */
  { 0x0422,  0x54, 0x00 },  /* Т to T */
  { 0x0423,  0x55, 0x00 },  /* У to U */
  { 0x0424,  0x46, 0x00 },  /* Ф to F */
  { 0x0425,  0x4b, 0x68 },  /* Х to Kh */
  { 0x0426,  0x54, 0x63 },  /* Ц to Tc */
  { 0x0427,  0x43, 0x68 },  /* Ч to Ch */
  { 0x0428,  0x53, 0x68 },  /* Ш to Sh */
  { 0x0429,  0x53, 0x68 },  /* Щ to Shch */
  { 0x042B,  0x59, 0x00 },  /* Ы to Y */
  { 0x042D,  0x45, 0x00 },  /* Э to E */
  { 0x042E,  0x49, 0x75 },  /* Ю to Iu */
  { 0x042F,  0x49, 0x61 },  /* Я to Ia */
  { 0x0430,  0x61, 0x00 },  /* а to a */
  { 0x0431,  0x62, 0x00 },  /* б to b */
  { 0x0432,  0x76, 0x00 },  /* в to v */
  { 0x0433,  0x67, 0x00 },  /* г to g */
  { 0x0434,  0x64, 0x00 },  /* д to d */
  { 0x0435,  0x65, 0x00 },  /* е to e */
  { 0x0436,  0x7a, 0x68 },  /* ж to zh */
  { 0x0437,  0x7a, 0x00 },  /* з to z */
  { 0x0438,  0x69, 0x00 },  /* и to i */
  { 0x0439,  0x69, 0x00 },  /* й to i */
  { 0x043A,  0x6b, 0x00 },  /* к to k */
  { 0x043B,  0x6c, 0x00 },  /* л to l */
  { 0x043C,  0x6d, 0x00 },  /* м to m */
  { 0x043D,  0x6e, 0x00 },  /* н to n */
  { 0x043E,  0x6f, 0x00 },  /* о to o */
  { 0x043F,  0x70, 0x00 },  /* п to p */
  { 0x0440,  0x72, 0x00 },  /* р to r */
  { 0x0441,  0x73, 0x00 },  /* с to s */
  { 0x0442,  0x74, 0x00 },  /* т to t */
  { 0x0443,  0x75, 0x00 },  /* у to u */
  { 0x0444,  0x66, 0x00 },  /* ф to f */
  { 0x0445,  0x6b, 0x68 },  /* х to kh */
  { 0x0446,  0x74, 0x63 },  /* ц to tc */
  { 0x0447,  0x63, 0x68 },  /* ч to ch */
  { 0x0448,  0x73, 0x68 },  /* ш to sh */
  { 0x0449,  0x73, 0x68 },  /* щ to shch */
  { 0x044B,  0x79, 0x00 },  /* ы to y */
  { 0x044D,  0x65, 0x00 },  /* э to e */
  { 0x044E,  0x69, 0x75 },  /* ю to iu */
  { 0x044F,  0x69, 0x61 },  /* я to ia */
  { 0x0450,  0x65, 0x00 },  /* ѐ to e */
  { 0x0451,  0x65, 0x00 },  /* ё to e */
  { 0x0452,  0x64, 0x00 },  /* ђ to d */
  { 0x0453,  0x67, 0x00 },  /* ѓ to g */
  { 0x0454,  0x65, 0x00 },  /* є to e */
  { 0x0455,  0x7a, 0x00 },  /* ѕ to z */
  { 0x0456,  0x69, 0x00 },  /* і to i */
  { 0x0457,  0x69, 0x00 },  /* ї to i */
  { 0x0458,  0x6a, 0x00 },  /* ј to j */
  { 0x0459,  0x69, 0x00 },  /* љ to i */
  { 0x045A,  0x6e, 0x00 },  /* њ to n */
  { 0x045B,  0x64, 0x00 },  /* ћ to d */
  { 0x045C,  0x6b, 0x00 },  /* ќ to k */
  { 0x045D,  0x69, 0x00 },  /* ѝ to i */
  { 0x045E,  0x75, 0x00 },  /* ў to u */
  { 0x045F,  0x64, 0x00 },  /* џ to d */
  { 0x1E02,  0x42, 0x00 },  /* Ḃ to B */
  { 0x1E03,  0x62, 0x00 },  /* ḃ to b */
  { 0x1E0A,  0x44, 0x00 },  /* Ḋ to D */
  { 0x1E0B,  0x64, 0x00 },  /* ḋ to d */
  { 0x1E1E,  0x46, 0x00 },  /* Ḟ to F */
  { 0x1E1F,  0x66, 0x00 },  /* ḟ to f */
  { 0x1E40,  0x4D, 0x00 },  /* Ṁ to M */
  { 0x1E41,  0x6D, 0x00 },  /* ṁ to m */
  { 0x1E56,  0x50, 0x00 },  /* Ṗ to P */
  { 0x1E57,  0x70, 0x00 },  /* ṗ to p */
  { 0x1E60,  0x53, 0x00 },  /* Ṡ to S */
  { 0x1E61,  0x73, 0x00 },  /* ṡ to s */
  { 0x1E6A,  0x54, 0x00 },  /* Ṫ to T */
  { 0x1E6B,  0x74, 0x00 },  /* ṫ to t */
  { 0x1E80,  0x57, 0x00 },  /* Ẁ to W */
  { 0x1E81,  0x77, 0x00 },  /* ẁ to w */
  { 0x1E82,  0x57, 0x00 },  /* Ẃ to W */
  { 0x1E83,  0x77, 0x00 },  /* ẃ to w */
  { 0x1E84,  0x57, 0x00 },  /* Ẅ to W */
  { 0x1E85,  0x77, 0x00 },  /* ẅ to w */
  { 0x1EF2,  0x59, 0x00 },  /* Ỳ to Y */
  { 0x1EF3,  0x79, 0x00 },  /* ỳ to y */
  { 0xFB00,  0x66, 0x66 },  /* ﬀ to ff */
  { 0xFB01,  0x66, 0x69 },  /* ﬁ to fi */
  { 0xFB02,  0x66, 0x6C },  /* ﬂ to fl */
  { 0xFB05,  0x73, 0x74 },  /* ﬅ to st */
  { 0xFB06,  0x73, 0x74 },  /* ﬆ to st */
};

/*
** Convert the input string from UTF-8 into pure ASCII by converting
** all non-ASCII characters to some combination of characters in the
** ASCII subset.
**
** The returned string might contain more characters than the input.
**
** Space to hold the returned string comes from sqlite3_malloc() and
** should be freed by the caller.
*/
static unsigned char *transliterate(const unsigned char *zIn, int nIn){
  unsigned char *zOut = sqlite3_malloc( nIn*4 + 1 );
  int i, c, sz, nOut;
  if( zOut==0 ) return 0;
  i = nOut = 0;
  while( i<nIn ){
    c = utf8Read(zIn, nIn, &sz);
    zIn += sz;
    nIn -= sz;
    if( c<=127 ){
      zOut[nOut++] = c;
    }else{
      int xTop, xBtm, x;
      xTop = sizeof(translit)/sizeof(translit[0]) - 1;
      xBtm = 0;
      while( xTop>=xBtm ){
        x = (xTop + xBtm)/2;
        if( translit[x].cFrom==c ){
          zOut[nOut++] = translit[x].cTo0;
          if( translit[x].cTo1 ){
            zOut[nOut++] = translit[x].cTo1;
            /* Add an extra "ch" after the "sh" for Щ and щ */
            if( c==0x0429 || c== 0x0449 ){
              zOut[nOut++] = 'c';
              zOut[nOut++] = 'h';
            }
          }
          c = 0;
          break;
        }else if( translit[x].cFrom>c ){
          xTop = x-1;
        }else{
          xBtm = x+1;
        }
      }
      if( c ) zOut[nOut++] = '?';
    }
  }
  zOut[nOut] = 0;
  return zOut;
}

/*
**    spellfix1_translit(X)
**
** Convert a string that contains non-ASCII Roman characters into 
** pure ASCII.
*/
static void transliterateSqlFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zIn = sqlite3_value_text(argv[0]);
  int nIn = sqlite3_value_bytes(argv[0]);
  unsigned char *zOut = transliterate(zIn, nIn);
  if( zOut==0 ){
    sqlite3_result_error_nomem(context);
  }else{
    sqlite3_result_text(context, (char*)zOut, -1, sqlite3_free);
  }
}

/*
**    spellfix1_scriptcode(X)
**
** Try to determine the dominant script used by the word X and return
** its ISO 15924 numeric code.
**
** The current implementation only understands the following scripts:
**
**    215  (Latin)
**    220  (Cyrillic)
**    200  (Greek)
**
** This routine will return 998 if the input X contains characters from
** two or more of the above scripts or 999 if X contains no characters
** from any of the above scripts.
*/
static void scriptCodeSqlFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zIn = sqlite3_value_text(argv[0]);
  int nIn = sqlite3_value_bytes(argv[0]);
  int c, sz;
  int scriptMask = 0;
  int res;
# define SCRIPT_LATIN       0x0001
# define SCRIPT_CYRILLIC    0x0002
# define SCRIPT_GREEK       0x0004

  while( nIn>0 ){
    c = utf8Read(zIn, nIn, &sz);
    zIn += sz;
    nIn -= sz;
    if( c<0x02af ){
      scriptMask |= SCRIPT_LATIN;
    }else if( c>=0x0400 && c<=0x04ff ){
      scriptMask |= SCRIPT_CYRILLIC;
    }else if( c>=0x0386 && c<=0x03ce ){
      scriptMask |= SCRIPT_GREEK;
    }
  }
  switch( scriptMask ){
    case 0:                res = 999; break;
    case SCRIPT_LATIN:     res = 215; break;
    case SCRIPT_CYRILLIC:  res = 220; break;
    case SCRIPT_GREEK:     res = 200; break;
    default:               res = 998; break;
  }
  sqlite3_result_int(context, res);
}

/*****************************************************************************
** Fuzzy-search virtual table
*****************************************************************************/

typedef struct spellfix1_vtab spellfix1_vtab;
typedef struct spellfix1_cursor spellfix1_cursor;

/* Fuzzy-search virtual table object */
struct spellfix1_vtab {
  sqlite3_vtab base;      /* Base class - must be first */
  sqlite3 *db;            /* Database connection */
  char *zDbName;          /* Name of database holding this table */
  char *zTableName;       /* Name of the virtual table */
};

/* Fuzzy-search cursor object */
struct spellfix1_cursor {
  sqlite3_vtab_cursor base;    /* Base class - must be first */
  spellfix1_vtab *pVTab;         /* The table to which this cursor belongs */
  int nRow;                    /* Number of rows of content */
  int nAlloc;                  /* Number of allocated rows */
  int iRow;                    /* Current row of content */
  int iLang;                   /* Value of the lang= constraint */
  int iTop;                    /* Value of the top= constraint */
  int iScope;                  /* Value of the scope= constraint */
  int nSearch;                 /* Number of vocabulary items checked */
  struct spellfix1_row {         /* For each row of content */
    sqlite3_int64 iRowid;         /* Rowid for this row */
    char *zWord;                  /* Text for this row */
    int iRank;                    /* Rank for this row */
    int iDistance;                /* Distance from pattern for this row */
    int iScore;                   /* Score for sorting */
  } *a; 
};

/*
** Construct one or more SQL statements from the format string given
** and then evaluate those statements. The success code is written
** into *pRc.
**
** If *pRc is initially non-zero then this routine is a no-op.
*/
static void spellfix1DbExec(
  int *pRc,              /* Success code */
  sqlite3 *db,           /* Database in which to run SQL */
  const char *zFormat,   /* Format string for SQL */
  ...                    /* Arguments to the format string */
){
  va_list ap;
  char *zSql;
  if( *pRc ) return;
  va_start(ap, zFormat);
  zSql = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  if( zSql==0 ){
    *pRc = SQLITE_NOMEM;
  }else{
    *pRc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
  }
}

/*
** xDisconnect/xDestroy method for the fuzzy-search module.
*/
static int spellfix1Uninit(int isDestroy, sqlite3_vtab *pVTab){
  spellfix1_vtab *p = (spellfix1_vtab*)pVTab;
  int rc = SQLITE_OK;
  if( isDestroy ){
    sqlite3 *db = p->db;
    spellfix1DbExec(&rc, db, "DROP TABLE IF EXISTS \"%w\".\"%w_vocab\"",
                  p->zDbName, p->zTableName);
  }
  if( rc==SQLITE_OK ){
    sqlite3_free(p->zTableName);
    sqlite3_free(p);
  }
  return rc;
}
static int spellfix1Disconnect(sqlite3_vtab *pVTab){
  return spellfix1Uninit(0, pVTab);
}
static int spellfix1Destroy(sqlite3_vtab *pVTab){
  return spellfix1Uninit(1, pVTab);
}

/*
** xConnect/xCreate method for the spellfix1 module. Arguments are:
**
**   argv[0]   -> module name  ("spellfix1")
**   argv[1]   -> database name
**   argv[2]   -> table name
**   argv[3].. -> optional arguments (currently ignored)
*/
static int spellfix1Init(
  int isCreate,
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVTab,
  char **pzErr
){
  spellfix1_vtab *pNew = 0;
  const char *zModule = argv[0];
  const char *zDbName = argv[1];
  const char *zTableName = argv[2];
  int nDbName;
  int rc = SQLITE_OK;

  if( argc<3 ){
    *pzErr = sqlite3_mprintf(
        "%s: wrong number of CREATE VIRTUAL TABLE arguments", argv[0]
    );
    rc = SQLITE_ERROR;
  }else{
    nDbName = strlen(zDbName);
    pNew = sqlite3_malloc( sizeof(*pNew) + nDbName + 1);
    if( pNew==0 ){
      rc = SQLITE_NOMEM;
    }else{
      memset(pNew, 0, sizeof(*pNew));
      pNew->zDbName = (char*)&pNew[1];
      memcpy(pNew->zDbName, zDbName, nDbName+1);
      pNew->zTableName = sqlite3_mprintf("%s", zTableName);
      pNew->db = db;
      if( pNew->zTableName==0 ){
        rc = SQLITE_NOMEM;
      }else{
        rc = sqlite3_declare_vtab(db, 
             "CREATE TABLE x(word,rank,distance,langid,"
             "score,top HIDDEN,scope HIDDEN,srchcnt HIDDEN,"
             "soundslike HIDDEN)"
        );
      }
      if( rc==SQLITE_OK && isCreate ){
        sqlite3_uint64 r;
        spellfix1DbExec(&rc, db,
           "CREATE TABLE IF NOT EXISTS \"%w\".\"%w_vocab\"(\n"
           "  id INTEGER PRIMARY KEY,\n"
           "  rank INT,\n"
           "  langid INT,\n"
           "  word TEXT,\n"
           "  k1 TEXT,\n"
           "  k2 TEXT\n"
           ");\n",
           zDbName, zTableName
        );
        sqlite3_randomness(sizeof(r), &r);
        spellfix1DbExec(&rc, db,
           "CREATE INDEX IF NOT EXISTS \"%w\".\"%w_index_%llx\" "
              "ON \"%w_vocab\"(langid,k2);",
           zDbName, zModule, r, zTableName
        );
      }
    }
  }

  *ppVTab = (sqlite3_vtab *)pNew;
  return rc;
}

/*
** The xConnect and xCreate methods
*/
static int spellfix1Connect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVTab,
  char **pzErr
){
  return spellfix1Init(0, db, pAux, argc, argv, ppVTab, pzErr);
}
static int spellfix1Create(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVTab,
  char **pzErr
){
  return spellfix1Init(1, db, pAux, argc, argv, ppVTab, pzErr);
}

/*
** Reset a cursor so that it contains zero rows of content but holds
** space for N rows.
*/
static void spellfix1ResetCursor(spellfix1_cursor *pCur, int N){
  int i;
  for(i=0; i<pCur->nRow; i++){
    sqlite3_free(pCur->a[i].zWord);
  }
  pCur->a = sqlite3_realloc(pCur->a, sizeof(pCur->a[0])*N);
  pCur->nAlloc = N;
  pCur->nRow = 0;
  pCur->iRow = 0;
  pCur->nSearch = 0;
}

/*
** Close a fuzzy-search cursor.
*/
static int spellfix1Close(sqlite3_vtab_cursor *cur){
  spellfix1_cursor *pCur = (spellfix1_cursor *)cur;
  spellfix1ResetCursor(pCur, 0);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/*
** Search for terms of these forms:
**
**   (A)    word MATCH $str
**   (B)    langid == $langid
**   (C)    top = $top
**   (D)    scope = $scope
**
** The plan number is a bit mask formed with these bits:
**
**   0x01   (A) is found
**   0x02   (B) is found
**   0x04   (C) is found
**   0x08   (D) is found
**
** filter.argv[*] values contains $str, $langid, $top, and $scope,
** if specified and in that order.
*/
static int spellfix1BestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  int iPlan = 0;
  int iLangTerm = -1;
  int iTopTerm = -1;
  int iScopeTerm = -1;
  int i;
  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;

    /* Terms of the form:  word MATCH $str */
    if( (iPlan & 1)==0 
     && pConstraint->iColumn==0
     && pConstraint->op==SQLITE_INDEX_CONSTRAINT_MATCH
    ){
      iPlan |= 1;
      pIdxInfo->aConstraintUsage[i].argvIndex = 1;
      pIdxInfo->aConstraintUsage[i].omit = 1;
    }

    /* Terms of the form:  langid = $langid  */
    if( (iPlan & 2)==0
     && pConstraint->iColumn==3
     && pConstraint->op==SQLITE_INDEX_CONSTRAINT_EQ
    ){
      iPlan |= 2;
      iLangTerm = i;
    }

    /* Terms of the form:  top = $top */
    if( (iPlan & 4)==0
     && pConstraint->iColumn==5
     && pConstraint->op==SQLITE_INDEX_CONSTRAINT_EQ
    ){
      iPlan |= 4;
      iTopTerm = i;
    }

    /* Terms of the form:  scope = $scope */
    if( (iPlan & 8)==0
     && pConstraint->iColumn==6
     && pConstraint->op==SQLITE_INDEX_CONSTRAINT_EQ
    ){
      iPlan |= 8;
      iScopeTerm = i;
    }
  }
  if( iPlan&1 ){
    int idx = 2;
    pIdxInfo->idxNum = iPlan;
    if( pIdxInfo->nOrderBy==1
     && pIdxInfo->aOrderBy[0].iColumn==4
     && pIdxInfo->aOrderBy[0].desc==0
    ){
      pIdxInfo->orderByConsumed = 1;  /* Default order by iScore */
    }
    if( iPlan&2 ){
      pIdxInfo->aConstraintUsage[iLangTerm].argvIndex = idx++;
      pIdxInfo->aConstraintUsage[iLangTerm].omit = 1;
    }
    if( iPlan&4 ){
      pIdxInfo->aConstraintUsage[iTopTerm].argvIndex = idx++;
      pIdxInfo->aConstraintUsage[iTopTerm].omit = 1;
    }
    if( iPlan&8 ){
      pIdxInfo->aConstraintUsage[iScopeTerm].argvIndex = idx++;
      pIdxInfo->aConstraintUsage[iScopeTerm].omit = 1;
    }
    pIdxInfo->estimatedCost = (double)10000;
  }else{
    pIdxInfo->idxNum = 0;
    pIdxInfo->estimatedCost = (double)10000000;
  }
  return SQLITE_OK;
}

/*
** Open a new fuzzy-search cursor.
*/
static int spellfix1Open(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  spellfix1_vtab *p = (spellfix1_vtab*)pVTab;
  spellfix1_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  pCur->pVTab = p;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Adjust a distance measurement by the words rank in order to show
** preference to common words.
*/
static int spellfix1Score(int iDistance, int iRank){
  int iLog2;
  for(iLog2=0; iRank>0; iLog2++, iRank>>=1){}
  return iDistance + 32 - iLog2;
}

/*
** Compare two spellfix1_row objects for sorting purposes in qsort() such
** that they sort in order of increasing distance.
*/
static int spellfix1RowCompare(const void *A, const void *B){
  const struct spellfix1_row *a = (const struct spellfix1_row*)A;
  const struct spellfix1_row *b = (const struct spellfix1_row*)B;
  return a->iScore - b->iScore;
}

/*
** This version of the xFilter method work if the MATCH term is present
** and we are doing a scan.
*/
static int spellfix1FilterForMatch(
  spellfix1_cursor *pCur,
  int idxNum,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zPatternIn;
  char *zPattern;
  int nPattern;
  char *zClass;
  int nClass;
  int iLimit = 20;
  int iScope = 4;
  int iLang = 0;
  char *zSql;
  int rc;
  sqlite3_stmt *pStmt;
  int idx = 1;
  spellfix1_vtab *p = pCur->pVTab;

  if( idxNum&2 ){
    iLang = sqlite3_value_int(argv[idx++]);
  }
  if( idxNum&4 ){
    iLimit = sqlite3_value_int(argv[idx++]);
    if( iLimit<1 ) iLimit = 1;
  }
  if( idxNum&8 ){
    iScope = sqlite3_value_int(argv[idx++]);
    if( iScope<1 ) iScope = 1;
  }
  spellfix1ResetCursor(pCur, iLimit);
  zPatternIn = sqlite3_value_text(argv[0]);
  if( zPatternIn==0 ) return SQLITE_OK;
  zPattern = (char*)transliterate(zPatternIn, sqlite3_value_bytes(argv[0]));
  if( zPattern==0 ) return SQLITE_NOMEM;
  nPattern = strlen(zPattern);
  if( zPattern[nPattern-1]=='*' ) nPattern--;
  if( nPattern<iScope ) iScope = nPattern;
  zClass = (char*)characterClassString((unsigned char*)zPattern,
                                       strlen(zPattern));
  nClass = strlen(zClass);
  if( nClass>iScope ){
    zClass[iScope] = 0;
    nClass = iScope;
  }
  zSql = sqlite3_mprintf(
     "SELECT id, word, rank, k1"
     "  FROM \"%w\".\"%w_vocab\""
     " WHERE langid=%d AND k2 GLOB '%q*'",
     p->zDbName, p->zTableName, iLang, zClass
  );
  rc = sqlite3_prepare_v2(p->db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if( rc==SQLITE_OK ){
    const char *zK1;
    int iDist;
    int iRank;
    int iScore;
    int iWorst = 999999999;
    int idx;
    int idxWorst;
    int i;

    while( sqlite3_step(pStmt)==SQLITE_ROW ){
      zK1 = (const char*)sqlite3_column_text(pStmt, 3);
      if( zK1==0 ) continue;
      pCur->nSearch++;
      iRank = sqlite3_column_int(pStmt, 2);
      iDist = editdist(zPattern, zK1);
      iScore = spellfix1Score(iDist,iRank);
      if( pCur->nRow<pCur->nAlloc ){
        idx = pCur->nRow;
      }else if( iScore<iWorst ){
        idx = idxWorst;
        sqlite3_free(pCur->a[idx].zWord);
      }else{
        continue;
      }
      pCur->a[idx].zWord = sqlite3_mprintf("%s", sqlite3_column_text(pStmt, 1));
      pCur->a[idx].iRowid = sqlite3_column_int64(pStmt, 0);
      pCur->a[idx].iRank = iRank;
      pCur->a[idx].iDistance = iDist;
      pCur->a[idx].iScore = iScore;
      if( pCur->nRow<pCur->nAlloc ) pCur->nRow++;
      if( pCur->nRow==pCur->nAlloc ){
        iWorst = pCur->a[0].iScore;
        idxWorst = 0;
        for(i=1; i<pCur->nRow; i++){
          iScore = pCur->a[i].iScore;
          if( iWorst<iScore ){
            iWorst = iScore;
            idxWorst = i;
          }
        }
      }
    }
  }
  qsort(pCur->a, pCur->nRow, sizeof(pCur->a[0]), spellfix1RowCompare);
  pCur->iTop = iLimit;
  pCur->iScope = iScope;
  sqlite3_finalize(pStmt);
  sqlite3_free(zPattern);
  sqlite3_free(zClass);
  return SQLITE_OK;
}

/*
** This version of xFilter handles a full-table scan case
*/
static int spellfix1FilterForFullScan(
  spellfix1_cursor *pCur,
  int idxNum,
  int argc,
  sqlite3_value **argv
){
  spellfix1ResetCursor(pCur, 0);
  return SQLITE_OK;
}


/*
** Called to "rewind" a cursor back to the beginning so that
** it starts its output over again.  Always called at least once
** prior to any spellfix1Column, spellfix1Rowid, or spellfix1Eof call.
*/
static int spellfix1Filter(
  sqlite3_vtab_cursor *cur, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  spellfix1_cursor *pCur = (spellfix1_cursor *)cur;
  int rc;
  if( idxNum & 1 ){
    rc = spellfix1FilterForMatch(pCur, idxNum, argc, argv);
  }else{
    rc = spellfix1FilterForFullScan(pCur, idxNum, argc, argv);
  }
  return rc;
}


/*
** Advance a cursor to its next row of output
*/
static int spellfix1Next(sqlite3_vtab_cursor *cur){
  spellfix1_cursor *pCur = (spellfix1_cursor *)cur;
  if( pCur->iRow < pCur->nRow ) pCur->iRow++;
  return SQLITE_OK;
}

/*
** Return TRUE if we are at the end-of-file
*/
static int spellfix1Eof(sqlite3_vtab_cursor *cur){
  spellfix1_cursor *pCur = (spellfix1_cursor *)cur;
  return pCur->iRow>=pCur->nRow;
}

/*
** Return columns from the current row.
*/
static int spellfix1Column(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
  spellfix1_cursor *pCur = (spellfix1_cursor*)cur;
  switch( i ){
    case 0: {
      sqlite3_result_text(ctx, pCur->a[pCur->iRow].zWord, -1, SQLITE_STATIC);
      break;
    }
    case 1: {
      sqlite3_result_int(ctx, pCur->a[pCur->iRow].iRank);
      break;
    }
    case 2: {
      sqlite3_result_int(ctx, pCur->a[pCur->iRow].iDistance);
      break;
    }
    case 3: {
      sqlite3_result_int(ctx, pCur->iLang);
      break;
    }
    case 4: {
      sqlite3_result_int(ctx, pCur->a[pCur->iRow].iScore);
      break;
    }
    case 5: {
      sqlite3_result_int(ctx, pCur->iTop);
      break;
    }
    case 6: {
      sqlite3_result_int(ctx, pCur->iScope);
      break;
    }
    case 7: {
      sqlite3_result_int(ctx, pCur->nSearch);
      break;
    }
    default: {
      sqlite3_result_null(ctx);
      break;
    }
  }
  return SQLITE_OK;
}

/*
** The rowid.
*/
static int spellfix1Rowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  spellfix1_cursor *pCur = (spellfix1_cursor*)cur;
  *pRowid = pCur->a[pCur->iRow].iRowid;
  return SQLITE_OK;
}

/*
** The xUpdate() method.
*/
static int spellfix1Update(
  sqlite3_vtab *pVTab,
  int argc,
  sqlite3_value **argv,
  sqlite_int64 *pRowid
){
  int rc = SQLITE_OK;
  sqlite3_int64 rowid, newRowid;
  spellfix1_vtab *p = (spellfix1_vtab*)pVTab;
  sqlite3 *db = p->db;

  if( argc==1 ){
    /* A delete operation on the rowid given by argv[0] */
    rowid = *pRowid = sqlite3_value_int64(argv[0]);
    spellfix1DbExec(&rc, db, "DELETE FROM \"%w\".\"%w_vocab\" "
                           " WHERE id=%lld",
                  p->zDbName, p->zTableName, rowid);
  }else{
    const unsigned char *zWord = sqlite3_value_text(argv[2]);
    int nWord = sqlite3_value_bytes(argv[2]);
    int iLang = sqlite3_value_int(argv[5]);
    int iRank = sqlite3_value_int(argv[3]);
    const unsigned char *zSoundslike = sqlite3_value_text(argv[10]);
    int nSoundslike = sqlite3_value_bytes(argv[10]);
    char *zK1, *zK2;
    int i;
    char c;

    if( zWord==0 ){
      pVTab->zErrMsg = sqlite3_mprintf("%w.word may not be NULL",
                            p->zTableName);
      return SQLITE_CONSTRAINT;
    }
    if( iRank<1 ) iRank = 1;
    if( zSoundslike ){
      zK1 = (char*)transliterate(zSoundslike, nSoundslike);
    }else{
      zK1 = (char*)transliterate(zWord, nWord);
    }
    if( zK1==0 ) return SQLITE_NOMEM;
    for(i=0; (c = zK1[i])!=0; i++){
       if( c>='A' && c<='Z' ) zK1[i] += 'a' - 'A';
    }
    zK2 = (char*)characterClassString((const unsigned char*)zK1, i);
    if( zK2==0 ){
      sqlite3_free(zK1);
      return SQLITE_NOMEM;
    }
    if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
      spellfix1DbExec(&rc, db,
             "INSERT INTO \"%w\".\"%w_vocab\"(rank,langid,word,k1,k2) "
             "VALUES(%d,%d,%Q,%Q,%Q)",
             p->zDbName, p->zTableName,
             iRank, iLang, zWord, zK1, zK2
      );
      *pRowid = sqlite3_last_insert_rowid(db);
    }else{
      rowid = sqlite3_value_int64(argv[0]);
      newRowid = *pRowid = sqlite3_value_int64(argv[1]);
      spellfix1DbExec(&rc, db,
             "UPDATE \"%w\".\"%w_vocab\" SET id=%lld, rank=%d, lang=%d,"
             " word=%Q, rank=%d, k1=%Q, k2=%Q WHERE id=%lld",
             p->zDbName, p->zTableName, newRowid, iRank, iLang,
             zWord, zK1, zK2, rowid
      );
    }
    sqlite3_free(zK1);
    sqlite3_free(zK2);
  }
  return rc;
}

/*
** Rename the spellfix1 table.
*/
static int spellfix1Rename(sqlite3_vtab *pVTab, const char *zNew){
  spellfix1_vtab *p = (spellfix1_vtab*)pVTab;
  sqlite3 *db = p->db;
  int rc = SQLITE_OK;
  char *zNewName = sqlite3_mprintf("%s", zNew);
  if( zNewName==0 ){
    return SQLITE_NOMEM;
  }
  spellfix1DbExec(&rc, db, 
     "ALTER TABLE \"%w\".\"%w_vocab\" RENAME TO \"%w_vocab\"",
     p->zDbName, p->zTableName, zNewName
  );
  if( rc==SQLITE_OK ){
    sqlite3_free(p->zTableName);
    p->zTableName = zNewName;
  }
  return rc;
}


/*
** A virtual table module that provides fuzzy search.
*/
static sqlite3_module spellfix1Module = {
  0,                       /* iVersion */
  spellfix1Create,         /* xCreate - handle CREATE VIRTUAL TABLE */
  spellfix1Connect,        /* xConnect - reconnected to an existing table */
  spellfix1BestIndex,      /* xBestIndex - figure out how to do a query */
  spellfix1Disconnect,     /* xDisconnect - close a connection */
  spellfix1Destroy,        /* xDestroy - handle DROP TABLE */
  spellfix1Open,           /* xOpen - open a cursor */
  spellfix1Close,          /* xClose - close a cursor */
  spellfix1Filter,         /* xFilter - configure scan constraints */
  spellfix1Next,           /* xNext - advance a cursor */
  spellfix1Eof,            /* xEof - check for end of scan */
  spellfix1Column,         /* xColumn - read data */
  spellfix1Rowid,          /* xRowid - read data */
  spellfix1Update,         /* xUpdate */
  0,                       /* xBegin */
  0,                       /* xSync */
  0,                       /* xCommit */
  0,                       /* xRollback */
  0,                       /* xFindMethod */
  spellfix1Rename,         /* xRename */
};

/*
** Register the various functions and the virtual table.
*/
static int spellfix1Register(sqlite3 *db){
  int nErr = 0;
  int i;
  nErr += sqlite3_create_function(db, "spellfix1_translit", 1, SQLITE_UTF8, 0,
                                  transliterateSqlFunc, 0, 0);
  nErr += sqlite3_create_function(db, "spellfix1_editdist", 2, SQLITE_UTF8, 0,
                                  editdistSqlFunc, 0, 0);
  nErr += sqlite3_create_function(db, "spellfix1_charclass", 1, SQLITE_UTF8, 0,
                                  characterClassSqlFunc, 0, 0);
  nErr += sqlite3_create_function(db, "spellfix1_scriptcode", 1, SQLITE_UTF8, 0,
                                  scriptCodeSqlFunc, 0, 0);
  nErr += sqlite3_create_module(db, "spellfix1", &spellfix1Module, 0);

  /* Verify sanity of the translit[] table */
  for(i=0; i<sizeof(translit)/sizeof(translit[0])-1; i++){
    assert( translit[i].cFrom<translit[i+1].cFrom );
  }  

  return nErr ? SQLITE_ERROR : SQLITE_OK;
}

#if SQLITE_CORE || defined(SQLITE_TEST)
/*
** Register the spellfix1 virtual table and its associated functions.
*/
int sqlite3Spellfix1Register(sqlite3 *db){
  return spellfix1Register(db);
}
#endif


#if !SQLITE_CORE
/*
** Extension load function.
*/
int sqlite3_extension_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi);
  return spellfix1Register(db);
}
#endif /* !SQLITE_CORE */
