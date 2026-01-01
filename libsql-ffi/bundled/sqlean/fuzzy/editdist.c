// Originally from the spellfix SQLite exension, Public Domain
// https://www.sqlite.org/src/file/ext/misc/spellfix.c
// Modified by Anton Zhiyanov, https://github.com/nalgeon/sqlean/, MIT License

#include <assert.h>
#include <stdlib.h>

#include "fuzzy/common.h"

extern const unsigned char midClass[];
extern const unsigned char initClass[];
extern const unsigned char className[];

/*
** Return the character class number for a character given its
** context.
*/
static char characterClass(char cPrev, char c) {
    return cPrev == 0 ? initClass[c & 0x7f] : midClass[c & 0x7f];
}

/*
** Return the cost of inserting or deleting character c immediately
** following character cPrev.  If cPrev==0, that means c is the first
** character of the word.
*/
static int insertOrDeleteCost(char cPrev, char c, char cNext) {
    char classC = characterClass(cPrev, c);
    char classCprev;

    if (classC == CCLASS_SILENT) {
        /* Insert or delete "silent" characters such as H or W */
        return 1;
    }
    if (cPrev == c) {
        /* Repeated characters, or miss a repeat */
        return 10;
    }
    if (classC == CCLASS_VOWEL && (cPrev == 'r' || cNext == 'r')) {
        return 20; /* Insert a vowel before or after 'r' */
    }
    classCprev = characterClass(cPrev, cPrev);
    if (classC == classCprev) {
        if (classC == CCLASS_VOWEL) {
            /* Remove or add a new vowel to a vowel cluster */
            return 15;
        } else {
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
#define FINAL_INS_COST_DIV 4

/*
** Return the cost of substituting cTo in place of cFrom assuming
** the previous character is cPrev.  If cPrev==0 then cTo is the first
** character of the word.
*/
static int substituteCost(char cPrev, char cFrom, char cTo) {
    char classFrom, classTo;
    if (cFrom == cTo) {
        /* Exact match */
        return 0;
    }
    if (cFrom == (cTo ^ 0x20) && ((cTo >= 'A' && cTo <= 'Z') || (cTo >= 'a' && cTo <= 'z'))) {
        /* differ only in case */
        return 0;
    }
    classFrom = characterClass(cPrev, cFrom);
    classTo = characterClass(cPrev, cTo);
    if (classFrom == classTo) {
        /* Same character class */
        return 40;
    }
    if (classFrom >= CCLASS_B && classFrom <= CCLASS_Y && classTo >= CCLASS_B &&
        classTo <= CCLASS_Y) {
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
**
** If pnMatch is not NULL, then *pnMatch is set to the number of bytes
** of zB that matched the pattern in zA. If zA does not end with a '*',
** then this value is always the number of bytes in zB (i.e. strlen(zB)).
** If zA does end in a '*', then it is the number of bytes in the prefix
** of zB that was deemed to match zA.
*/
int edit_distance(const char* zA, const char* zB, int* pnMatch) {
    int nA, nB;          /* Number of characters in zA[] and zB[] */
    int xA, xB;          /* Loop counters for zA[] and zB[] */
    char cA = 0, cB;     /* Current character of zA and zB */
    char cAprev, cBprev; /* Previous character of zA and zB */
    char cAnext, cBnext; /* Next character in zA and zB */
    int d;               /* North-west cost value */
    int dc = 0;          /* North-west character value */
    int res;             /* Final result */
    int* m;              /* The cost matrix */
    char* cx;            /* Corresponding character values */
    int* toFree = 0;     /* Malloced space */
    int nMatch = 0;
    int mStack[60 + 15]; /* Stack space to use if not too much is needed */

    /* Early out if either input is NULL */
    if (zA == 0 || zB == 0)
        return -1;

    /* Skip any common prefix */
    while (zA[0] && zA[0] == zB[0]) {
        dc = zA[0];
        zA++;
        zB++;
        nMatch++;
    }
    if (pnMatch)
        *pnMatch = nMatch;
    if (zA[0] == 0 && zB[0] == 0)
        return 0;

#if 0
  printf("A=\"%s\" B=\"%s\" dc=%c\n", zA, zB, dc?dc:' ');
#endif

    /* Verify input strings and measure their lengths */
    for (nA = 0; zA[nA]; nA++) {
        if (zA[nA] & 0x80)
            return -2;
    }
    for (nB = 0; zB[nB]; nB++) {
        if (zB[nB] & 0x80)
            return -2;
    }

    /* Special processing if either string is empty */
    if (nA == 0) {
        cBprev = (char)dc;
        for (xB = res = 0; (cB = zB[xB]) != 0; xB++) {
            res += insertOrDeleteCost(cBprev, cB, zB[xB + 1]) / FINAL_INS_COST_DIV;
            cBprev = cB;
        }
        return res;
    }
    if (nB == 0) {
        cAprev = (char)dc;
        for (xA = res = 0; (cA = zA[xA]) != 0; xA++) {
            res += insertOrDeleteCost(cAprev, cA, zA[xA + 1]);
            cAprev = cA;
        }
        return res;
    }

    /* A is a prefix of B */
    if (zA[0] == '*' && zA[1] == 0)
        return 0;

    /* Allocate and initialize the Wagner matrix */
    if ((size_t)nB < (sizeof(mStack) * 4) / (sizeof(mStack[0]) * 5)) {
        m = mStack;
    } else {
        m = toFree = malloc((nB + 1) * 5 * sizeof(m[0]) / 4);
        if (m == 0)
            return -3;
    }
    cx = (char*)&m[nB + 1];

    /* Compute the Wagner edit distance */
    m[0] = 0;
    cx[0] = (char)dc;
    cBprev = (char)dc;
    for (xB = 1; xB <= nB; xB++) {
        cBnext = zB[xB];
        cB = zB[xB - 1];
        cx[xB] = cB;
        m[xB] = m[xB - 1] + insertOrDeleteCost(cBprev, cB, cBnext);
        cBprev = cB;
    }
    cAprev = (char)dc;
    for (xA = 1; xA <= nA; xA++) {
        int lastA = (xA == nA);
        cA = zA[xA - 1];
        cAnext = zA[xA];
        if (cA == '*' && lastA)
            break;
        d = m[0];
        dc = cx[0];
        m[0] = d + insertOrDeleteCost(cAprev, cA, cAnext);
        cBprev = 0;
        for (xB = 1; xB <= nB; xB++) {
            int totalCost, insCost, delCost, subCost, ncx;
            cB = zB[xB - 1];
            cBnext = zB[xB];

            /* Cost to insert cB */
            insCost = insertOrDeleteCost(cx[xB - 1], cB, cBnext);
            if (lastA)
                insCost /= FINAL_INS_COST_DIV;

            /* Cost to delete cA */
            delCost = insertOrDeleteCost(cx[xB], cA, cBnext);

            /* Cost to substitute cA->cB */
            subCost = substituteCost(cx[xB - 1], cA, cB);

            /* Best cost */
            totalCost = insCost + m[xB - 1];
            ncx = cB;
            if ((delCost + m[xB]) < totalCost) {
                totalCost = delCost + m[xB];
                ncx = cA;
            }
            if ((subCost + d) < totalCost) {
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
            cx[xB] = (char)ncx;
            cBprev = cB;
        }
        cAprev = cA;
    }

    /* Free the wagner matrix and return the result */
    if (cA == '*') {
        res = m[1];
        for (xB = 1; xB <= nB; xB++) {
            if (m[xB] < res) {
                res = m[xB];
                if (pnMatch)
                    *pnMatch = xB + nMatch;
            }
        }
    } else {
        res = m[nB];
        /* In the current implementation, pnMatch is always NULL if zA does
        ** not end in "*" */
        assert(pnMatch == 0);
    }
    free(toFree);
    return res;
}
