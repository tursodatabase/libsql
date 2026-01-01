// Originally from the spellfix SQLite exension, Public Domain
// https://www.sqlite.org/src/file/ext/misc/spellfix.c
// Modified by Anton Zhiyanov, https://github.com/nalgeon/sqlean/, MIT License

#include "fuzzy/common.h"

/*
** The following table gives the character class for non-initial ASCII
** characters.
*/
const unsigned char midClass[] = {
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_SPACE,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_SPACE,  /*   */ CCLASS_SPACE, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_SPACE,
    /* ! */ CCLASS_OTHER,  /* " */ CCLASS_OTHER, /* # */ CCLASS_OTHER,
    /* $ */ CCLASS_OTHER,  /* % */ CCLASS_OTHER, /* & */ CCLASS_OTHER,
    /* ' */ CCLASS_SILENT, /* ( */ CCLASS_OTHER, /* ) */ CCLASS_OTHER,
    /* * */ CCLASS_OTHER,  /* + */ CCLASS_OTHER, /* , */ CCLASS_OTHER,
    /* - */ CCLASS_OTHER,  /* . */ CCLASS_OTHER, /* / */ CCLASS_OTHER,
    /* 0 */ CCLASS_DIGIT,  /* 1 */ CCLASS_DIGIT, /* 2 */ CCLASS_DIGIT,
    /* 3 */ CCLASS_DIGIT,  /* 4 */ CCLASS_DIGIT, /* 5 */ CCLASS_DIGIT,
    /* 6 */ CCLASS_DIGIT,  /* 7 */ CCLASS_DIGIT, /* 8 */ CCLASS_DIGIT,
    /* 9 */ CCLASS_DIGIT,  /* : */ CCLASS_OTHER, /* ; */ CCLASS_OTHER,
    /* < */ CCLASS_OTHER,  /* = */ CCLASS_OTHER, /* > */ CCLASS_OTHER,
    /* ? */ CCLASS_OTHER,  /* @ */ CCLASS_OTHER, /* A */ CCLASS_VOWEL,
    /* B */ CCLASS_B,      /* C */ CCLASS_C,     /* D */ CCLASS_D,
    /* E */ CCLASS_VOWEL,  /* F */ CCLASS_B,     /* G */ CCLASS_C,
    /* H */ CCLASS_SILENT, /* I */ CCLASS_VOWEL, /* J */ CCLASS_C,
    /* K */ CCLASS_C,      /* L */ CCLASS_L,     /* M */ CCLASS_M,
    /* N */ CCLASS_M,      /* O */ CCLASS_VOWEL, /* P */ CCLASS_B,
    /* Q */ CCLASS_C,      /* R */ CCLASS_R,     /* S */ CCLASS_C,
    /* T */ CCLASS_D,      /* U */ CCLASS_VOWEL, /* V */ CCLASS_B,
    /* W */ CCLASS_B,      /* X */ CCLASS_C,     /* Y */ CCLASS_VOWEL,
    /* Z */ CCLASS_C,      /* [ */ CCLASS_OTHER, /* \ */ CCLASS_OTHER,
    /* ] */ CCLASS_OTHER,  /* ^ */ CCLASS_OTHER, /* _ */ CCLASS_OTHER,
    /* ` */ CCLASS_OTHER,  /* a */ CCLASS_VOWEL, /* b */ CCLASS_B,
    /* c */ CCLASS_C,      /* d */ CCLASS_D,     /* e */ CCLASS_VOWEL,
    /* f */ CCLASS_B,      /* g */ CCLASS_C,     /* h */ CCLASS_SILENT,
    /* i */ CCLASS_VOWEL,  /* j */ CCLASS_C,     /* k */ CCLASS_C,
    /* l */ CCLASS_L,      /* m */ CCLASS_M,     /* n */ CCLASS_M,
    /* o */ CCLASS_VOWEL,  /* p */ CCLASS_B,     /* q */ CCLASS_C,
    /* r */ CCLASS_R,      /* s */ CCLASS_C,     /* t */ CCLASS_D,
    /* u */ CCLASS_VOWEL,  /* v */ CCLASS_B,     /* w */ CCLASS_B,
    /* x */ CCLASS_C,      /* y */ CCLASS_VOWEL, /* z */ CCLASS_C,
    /* { */ CCLASS_OTHER,  /* | */ CCLASS_OTHER, /* } */ CCLASS_OTHER,
    /* ~ */ CCLASS_OTHER,  /*   */ CCLASS_OTHER,
};
/*
** This tables gives the character class for ASCII characters that form the
** initial character of a word.  The only difference from midClass is with
** the letters H, W, and Y.
*/
const unsigned char initClass[] = {
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_SPACE,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_SPACE,  /*   */ CCLASS_SPACE, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_OTHER,
    /*   */ CCLASS_OTHER,  /*   */ CCLASS_OTHER, /*   */ CCLASS_SPACE,
    /* ! */ CCLASS_OTHER,  /* " */ CCLASS_OTHER, /* # */ CCLASS_OTHER,
    /* $ */ CCLASS_OTHER,  /* % */ CCLASS_OTHER, /* & */ CCLASS_OTHER,
    /* ' */ CCLASS_OTHER,  /* ( */ CCLASS_OTHER, /* ) */ CCLASS_OTHER,
    /* * */ CCLASS_OTHER,  /* + */ CCLASS_OTHER, /* , */ CCLASS_OTHER,
    /* - */ CCLASS_OTHER,  /* . */ CCLASS_OTHER, /* / */ CCLASS_OTHER,
    /* 0 */ CCLASS_DIGIT,  /* 1 */ CCLASS_DIGIT, /* 2 */ CCLASS_DIGIT,
    /* 3 */ CCLASS_DIGIT,  /* 4 */ CCLASS_DIGIT, /* 5 */ CCLASS_DIGIT,
    /* 6 */ CCLASS_DIGIT,  /* 7 */ CCLASS_DIGIT, /* 8 */ CCLASS_DIGIT,
    /* 9 */ CCLASS_DIGIT,  /* : */ CCLASS_OTHER, /* ; */ CCLASS_OTHER,
    /* < */ CCLASS_OTHER,  /* = */ CCLASS_OTHER, /* > */ CCLASS_OTHER,
    /* ? */ CCLASS_OTHER,  /* @ */ CCLASS_OTHER, /* A */ CCLASS_VOWEL,
    /* B */ CCLASS_B,      /* C */ CCLASS_C,     /* D */ CCLASS_D,
    /* E */ CCLASS_VOWEL,  /* F */ CCLASS_B,     /* G */ CCLASS_C,
    /* H */ CCLASS_SILENT, /* I */ CCLASS_VOWEL, /* J */ CCLASS_C,
    /* K */ CCLASS_C,      /* L */ CCLASS_L,     /* M */ CCLASS_M,
    /* N */ CCLASS_M,      /* O */ CCLASS_VOWEL, /* P */ CCLASS_B,
    /* Q */ CCLASS_C,      /* R */ CCLASS_R,     /* S */ CCLASS_C,
    /* T */ CCLASS_D,      /* U */ CCLASS_VOWEL, /* V */ CCLASS_B,
    /* W */ CCLASS_B,      /* X */ CCLASS_C,     /* Y */ CCLASS_Y,
    /* Z */ CCLASS_C,      /* [ */ CCLASS_OTHER, /* \ */ CCLASS_OTHER,
    /* ] */ CCLASS_OTHER,  /* ^ */ CCLASS_OTHER, /* _ */ CCLASS_OTHER,
    /* ` */ CCLASS_OTHER,  /* a */ CCLASS_VOWEL, /* b */ CCLASS_B,
    /* c */ CCLASS_C,      /* d */ CCLASS_D,     /* e */ CCLASS_VOWEL,
    /* f */ CCLASS_B,      /* g */ CCLASS_C,     /* h */ CCLASS_SILENT,
    /* i */ CCLASS_VOWEL,  /* j */ CCLASS_C,     /* k */ CCLASS_C,
    /* l */ CCLASS_L,      /* m */ CCLASS_M,     /* n */ CCLASS_M,
    /* o */ CCLASS_VOWEL,  /* p */ CCLASS_B,     /* q */ CCLASS_C,
    /* r */ CCLASS_R,      /* s */ CCLASS_C,     /* t */ CCLASS_D,
    /* u */ CCLASS_VOWEL,  /* v */ CCLASS_B,     /* w */ CCLASS_B,
    /* x */ CCLASS_C,      /* y */ CCLASS_Y,     /* z */ CCLASS_C,
    /* { */ CCLASS_OTHER,  /* | */ CCLASS_OTHER, /* } */ CCLASS_OTHER,
    /* ~ */ CCLASS_OTHER,  /*   */ CCLASS_OTHER,
};

/*
** Mapping from the character class number (0-13) to a symbol for each
** character class.  Note that initClass[] can be used to map the class
** symbol back into the class number.
*/
const unsigned char className[] = ".ABCDHLRMY9 ?";
