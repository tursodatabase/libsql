// Ooriginally from the spellfix SQLite exension, Public Domain
// https://www.sqlite.org/src/file/ext/misc/spellfix.c
// Modified by Anton Zhiyanov, https://github.com/nalgeon/sqlean/, MIT License

#include <assert.h>
#include <stdlib.h>

#include "fuzzy/common.h"

extern const unsigned char midClass[];
extern const unsigned char initClass[];
extern const unsigned char className[];

/*
** Generate a "phonetic hash" from a string of ASCII characters
** in zIn[0..nIn-1].
**
**   * Map characters by character class as defined above.
**   * Omit double-letters
**   * Omit vowels beside R and L
**   * Omit T when followed by CH
**   * Omit W when followed by R
**   * Omit D when followed by J or G
**   * Omit K in KN or G in GN at the beginning of a word
**
** Space to hold the result is obtained from sqlite3_malloc()
**
** Return NULL if memory allocation fails.
*/
unsigned char* phonetic_hash(const unsigned char* zIn, int nIn) {
    unsigned char* zOut = malloc(nIn + 1);
    int i;
    int nOut = 0;
    char cPrev = 0x77;
    char cPrevX = 0x77;
    const unsigned char* aClass = initClass;

    if (zOut == 0)
        return 0;
    if (nIn > 2) {
        switch (zIn[0]) {
            case 'g':
            case 'k': {
                if (zIn[1] == 'n') {
                    zIn++;
                    nIn--;
                }
                break;
            }
        }
    }
    for (i = 0; i < nIn; i++) {
        unsigned char c = zIn[i];
        if (i + 1 < nIn) {
            if (c == 'w' && zIn[i + 1] == 'r')
                continue;
            if (c == 'd' && (zIn[i + 1] == 'j' || zIn[i + 1] == 'g'))
                continue;
            if (i + 2 < nIn) {
                if (c == 't' && zIn[i + 1] == 'c' && zIn[i + 2] == 'h')
                    continue;
            }
        }
        c = aClass[c & 0x7f];
        if (c == CCLASS_SPACE)
            continue;
        if (c == CCLASS_OTHER && cPrev != CCLASS_DIGIT)
            continue;
        aClass = midClass;
        if (c == CCLASS_VOWEL && (cPrevX == CCLASS_R || cPrevX == CCLASS_L)) {
            continue; /* No vowels beside L or R */
        }
        if ((c == CCLASS_R || c == CCLASS_L) && cPrevX == CCLASS_VOWEL) {
            nOut--; /* No vowels beside L or R */
        }
        cPrev = c;
        if (c == CCLASS_SILENT)
            continue;
        cPrevX = c;
        c = className[c];
        assert(nOut >= 0);
        if (nOut == 0 || c != zOut[nOut - 1])
            zOut[nOut++] = c;
    }
    zOut[nOut] = 0;
    return zOut;
}
