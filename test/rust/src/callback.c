#include "callback.h"

int callback(void *notUsed, int argc, char **argv, char **azColName) {
    FILE *fptr = fopen("./test_select_42.out", "w");
    assert(fptr != NULL);

    for(int i = 0; i < argc; i++) {
        fprintf(fptr, "%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    
    fclose(fptr);
    return 0;
}