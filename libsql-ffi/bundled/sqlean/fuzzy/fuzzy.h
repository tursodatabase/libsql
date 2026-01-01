// Copyright (c) 2014 Ross Bayer, MIT License
// https://github.com/Rostepher/libstrcmp

#ifndef FUZZY_H
#define FUZZY_H

// distance metrics
unsigned damerau_levenshtein(const char*, const char*);
int hamming(const char*, const char*);
double jaro(const char*, const char*);
double jaro_winkler(const char*, const char*);
unsigned levenshtein(const char*, const char*);
unsigned optimal_string_alignment(const char*, const char*);
int edit_distance(const char*, const char*, int*);

// phonetics
char* caverphone(const char*);
char* soundex(const char*);
char* refined_soundex(const char*);
unsigned char* phonetic_hash(const unsigned char*, int);

// translit
unsigned char* transliterate(const unsigned char*, int);
int translen_to_charlen(const char*, int, int);
int script_code(const unsigned char*, int);

#endif
