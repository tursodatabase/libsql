// Copyright (c) 2021 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Caverphone phonetic coding algorithm.
// https://en.wikipedia.org/wiki/Caverphone

#include <assert.h>
#include <stdlib.h>
#include <string.h>

// remove_non_letters deletes everything from the source string,
// except lowercased letters a-z
static char* remove_non_letters(const char* src) {
    size_t src_len = strlen(src);
    char* res = malloc((src_len + 1) * sizeof(char));
    const char* src_it;
    char* res_it = res;
    for (size_t idx = 0; idx < src_len; idx++) {
        src_it = src + idx;
        if (*src_it < 97 || *src_it > 122) {
            continue;
        }
        *res_it = *src_it;
        res_it++;
    }
    *res_it = '\0';
    return res;
}

// replace_start replaces the `old` substring with the `new` one
// if it matches at the beginning of the `src` string
static char* replace_start(const char* src, const char* old, const char* new) {
    size_t src_len = strlen(src);
    size_t old_len = strlen(old);
    size_t new_len = strlen(new);
    assert(new_len <= old_len);

    char* res = malloc((src_len + 1) * sizeof(char));

    if (src_len < old_len) {
        // source string is shorter than the substring to replace,
        // so there is definitely no match
        strcpy(res, src);
        return res;
    }

    if (strncmp(src, old, old_len) == 0) {
        strncpy(res, new, new_len);
        strncpy(res + new_len, src + old_len, src_len - old_len);
        *(res + src_len - old_len + new_len) = '\0';
    } else {
        strcpy(res, src);
    }
    return res;
}

// replace_end replaces the `old` substring with the `new` one
// if it matches at the end of the `src` string
static char* replace_end(const char* src, const char* old, const char* new) {
    size_t src_len = strlen(src);
    size_t old_len = strlen(old);
    size_t new_len = strlen(new);
    assert(new_len <= old_len);

    char* res = malloc((src_len + 1) * sizeof(char));

    if (src_len < old_len) {
        // source string is shorter than the substring to replace,
        // so there is definitely no match
        strcpy(res, src);
        return res;
    }

    strncpy(res, src, src_len - old_len);
    if (strncmp(src + src_len - old_len, old, old_len) == 0) {
        strncpy(res + src_len - old_len, new, new_len);
        *(res + src_len - old_len + new_len) = '\0';
    } else {
        strncpy(res + src_len - old_len, src + src_len - old_len, old_len);
        *(res + src_len) = '\0';
    }
    return res;
}

// replace replaces all `old` substrings with `new` ones
// in the the `src` string
static char* replace(const char* src, const char* old, const char* new) {
    size_t src_len = strlen(src);
    size_t old_len = strlen(old);
    size_t new_len = strlen(new);
    assert(new_len <= old_len);

    char* res = malloc((src_len + 1) * sizeof(char));

    if (src_len < old_len) {
        // source string is shorter than the substring to replace,
        // so there is definitely no match
        strcpy(res, src);
        return res;
    }

    const char* src_it;
    char* res_it = res;
    for (size_t idx = 0; idx < src_len;) {
        src_it = src + idx;
        if (strncmp(src_it, old, old_len) == 0) {
            strncpy(res_it, new, new_len);
            res_it += new_len;
            idx += old_len;
        } else {
            *res_it = *src_it;
            res_it++;
            idx++;
        }
    }
    *res_it = '\0';
    return res;
}

// replace_seq replaces all sequences of the `old` character
// with the `new` substring in the the `src` string
static char* replace_seq(const char* src, const char old, const char* new) {
    size_t src_len = strlen(src);
    size_t new_len = strlen(new);
    char* res = malloc((src_len + 1) * sizeof(char));
    const char* src_it;
    char* res_it = res;
    size_t match_len = 0;
    for (size_t idx = 0; idx < src_len;) {
        src_it = src + idx;
        if (*src_it == old) {
            match_len++;
            idx++;
        } else {
            if (match_len > 0) {
                strncpy(res_it, new, new_len);
                res_it += new_len;
                match_len = 0;
            }
            *res_it = *src_it;
            res_it++;
            idx++;
        }
    }
    if (match_len > 0) {
        strncpy(res_it, new, new_len);
        res_it += new_len;
    }
    *res_it = '\0';
    return res;
}

// pad pads `src` string with trailing 1s
// up to the length of 10 characters
static char* pad(const char* src) {
    size_t src_len = strlen(src);
    size_t max_len = 10;

    char* res = malloc((max_len + 1) * sizeof(char));
    strncpy(res, src, max_len);
    if (src_len < max_len) {
        for (size_t idx = src_len; idx < max_len; idx++) {
            *(res + idx) = '1';
        }
    }
    *(res + max_len) = '\0';
    return res;
}

// step frees the source string and returns the result one
static char* step(char* res, char* src) {
    free(src);
    return res;
}

// caverphone implements the Caverphone phonetic hashing algorithm
// as described in https://caversham.otago.ac.nz/files/working/ctp150804.pdf
char* caverphone(const char* src) {
    assert(src != NULL);

    char* res = malloc((strlen(src) + 1) * sizeof(char));

    if (src == 0 || *src == '\0') {
        res[0] = '\0';
        return res;
    }

    strcpy(res, src);

    // Remove anything not in the standard alphabet
    res = step(remove_non_letters((const char*)res), res);

    // Remove final e
    res = step(replace_end((const char*)res, "e", ""), res);

    // If the name starts with *gh make it *2f
    res = step(replace_start((const char*)res, "cough", "cou2f"), res);
    res = step(replace_start((const char*)res, "rough", "rou2f"), res);
    res = step(replace_start((const char*)res, "tough", "tou2f"), res);
    res = step(replace_start((const char*)res, "enough", "enou2f"), res);
    res = step(replace_start((const char*)res, "trough", "trou2f"), res);

    // If the name starts with gn make it 2n
    res = step(replace_start((const char*)res, "gn", "2n"), res);
    // If the name ends with mb make it m2
    res = step(replace_end((const char*)res, "mb", "m2"), res);
    // replace cq with 2q
    res = step(replace((const char*)res, "cq", "2q"), res);

    // replace c[iey] with s[iey]
    res = step(replace((const char*)res, "ci", "si"), res);
    res = step(replace((const char*)res, "ce", "se"), res);
    res = step(replace((const char*)res, "cy", "sy"), res);

    // replace tch with 2ch
    res = step(replace((const char*)res, "tch", "2ch"), res);

    // replace [cqx] with k
    res = step(replace((const char*)res, "c", "k"), res);
    res = step(replace((const char*)res, "q", "k"), res);
    res = step(replace((const char*)res, "x", "k"), res);

    // replace v with f
    res = step(replace((const char*)res, "v", "f"), res);
    // replace dg with 2g
    res = step(replace((const char*)res, "dg", "2g"), res);

    // replace ti[oa] with si[oa]
    res = step(replace((const char*)res, "tio", "sio"), res);
    res = step(replace((const char*)res, "tia", "sia"), res);

    // replace d with t
    res = step(replace((const char*)res, "d", "t"), res);
    // replace ph with fh
    res = step(replace((const char*)res, "ph", "fh"), res);
    // replace b with p
    res = step(replace((const char*)res, "b", "p"), res);
    // replace sh with s2
    res = step(replace((const char*)res, "sh", "s2"), res);
    // replace z with s
    res = step(replace((const char*)res, "z", "s"), res);

    // replace an initial vowel [aeiou] with an A
    res = step(replace_start((const char*)res, "a", "A"), res);
    res = step(replace_start((const char*)res, "e", "A"), res);
    res = step(replace_start((const char*)res, "i", "A"), res);
    res = step(replace_start((const char*)res, "o", "A"), res);
    res = step(replace_start((const char*)res, "u", "A"), res);

    // replace all other vowels with a 3
    res = step(replace((const char*)res, "a", "3"), res);
    res = step(replace((const char*)res, "e", "3"), res);
    res = step(replace((const char*)res, "i", "3"), res);
    res = step(replace((const char*)res, "o", "3"), res);
    res = step(replace((const char*)res, "u", "3"), res);

    // replace j with y
    res = step(replace((const char*)res, "j", "y"), res);

    // replace an initial y3 with Y3
    res = step(replace_start((const char*)res, "y3", "Y3"), res);
    // replace an initial y with A
    res = step(replace_start((const char*)res, "y", "A"), res);
    // replace y with 3
    res = step(replace((const char*)res, "y", "3"), res);

    // replace 3gh3 with 3kh3
    res = step(replace((const char*)res, "3gh3", "3kh3"), res);
    // replace gh with 22
    res = step(replace((const char*)res, "gh", "22"), res);
    // replace g with k
    res = step(replace((const char*)res, "g", "k"), res);

    // replace sequence of the letter [stpkfmn] with an uppercased letter
    res = step(replace_seq((const char*)res, 's', "S"), res);
    res = step(replace_seq((const char*)res, 't', "T"), res);
    res = step(replace_seq((const char*)res, 'p', "P"), res);
    res = step(replace_seq((const char*)res, 'k', "K"), res);
    res = step(replace_seq((const char*)res, 'f', "F"), res);
    res = step(replace_seq((const char*)res, 'm', "M"), res);
    res = step(replace_seq((const char*)res, 'n', "N"), res);

    // replace w3 with W3
    res = step(replace((const char*)res, "w3", "W3"), res);
    // replace wh3 with Wh3
    res = step(replace((const char*)res, "wh3", "Wh3"), res);
    // replace the final w with 3
    res = step(replace_end((const char*)res, "w", "3"), res);
    // replace w with 2
    res = step(replace((const char*)res, "w", "2"), res);

    // replace an initial h with an A
    res = step(replace_start((const char*)res, "h", "A"), res);
    // replace all other occurrences of h with a 2
    res = step(replace((const char*)res, "h", "2"), res);

    // replace r3 with R3
    res = step(replace((const char*)res, "r3", "R3"), res);
    // replace the final r with 3
    res = step(replace_end((const char*)res, "r", "3"), res);
    // replace r with 2
    res = step(replace((const char*)res, "r", "2"), res);

    // replace l3 with L3
    res = step(replace((const char*)res, "l3", "L3"), res);
    // replace the final l with 3
    res = step(replace_end((const char*)res, "l", "3"), res);
    // replace l with 2
    res = step(replace((const char*)res, "l", "2"), res);

    // remove all 2s
    res = step(replace((const char*)res, "2", ""), res);
    // replace the final 3 with A
    res = step(replace_end((const char*)res, "3", "A"), res);
    // remove all 3s
    res = step(replace((const char*)res, "3", ""), res);

    // put ten 1s on the end
    // take the first ten characters as the code
    res = step(pad((const char*)res), res);

    return res;
}
