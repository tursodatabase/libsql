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
** Header file for the Virtual DataBase Engine (VDBE)
**
** This header defines the interface to the virtual database engine
** or VDBE.  The VDBE implements an abstract machine that runs a
** simple program to access and modify the underlying database.
**
** $Id: vdbe.h,v 1.58 2002/08/25 19:20:42 drh Exp $
*/
#ifndef _SQLITE_VDBE_H_
#define _SQLITE_VDBE_H_
#include <stdio.h>

/*
** A single VDBE is an opaque structure named "Vdbe".  Only routines
** in the source file sqliteVdbe.c are allowed to see the insides
** of this structure.
*/
typedef struct Vdbe Vdbe;

/*
** A single instruction of the virtual machine has an opcode
** and as many as three operands.  The instruction is recorded
** as an instance of the following structure:
*/
struct VdbeOp {
  int opcode;         /* What operation to perform */
  int p1;             /* First operand */
  int p2;             /* Second parameter (often the jump destination) */
  char *p3;           /* Third parameter */
  int p3type;         /* P3_STATIC, P3_DYNAMIC or P3_POINTER */
};
typedef struct VdbeOp VdbeOp;

/*
** Allowed values of VdbeOp.p3type
*/
#define P3_NOTUSED    0   /* The P3 parameter is not used */
#define P3_DYNAMIC    1   /* Pointer to a string obtained from sqliteMalloc() */
#define P3_STATIC   (-1)  /* Pointer to a static string */
#define P3_POINTER  (-2)  /* P3 is a pointer to some structure or object */

/*
** The following macro converts a relative address in the p2 field
** of a VdbeOp structure into a negative number so that 
** sqliteVdbeAddOpList() knows that the address is relative.  Calling
** the macro again restores the address.
*/
#define ADDR(X)  (-1-(X))

/*
** These are the available opcodes.
**
** If any of the values changes or if opcodes are added or removed,
** be sure to also update the zOpName[] array in sqliteVdbe.c to
** mirror the change.
**
** The source tree contains an AWK script named renumberOps.awk that
** can be used to renumber these opcodes when new opcodes are inserted.
*/
#define OP_Transaction         1
#define OP_Checkpoint          2
#define OP_Commit              3
#define OP_Rollback            4

#define OP_ReadCookie          5
#define OP_SetCookie           6
#define OP_VerifyCookie        7

#define OP_Open                8
#define OP_OpenTemp            9
#define OP_OpenWrite          10
#define OP_OpenAux            11
#define OP_OpenWrAux          12
#define OP_RenameCursor       13
#define OP_Close              14
#define OP_MoveTo             15
#define OP_NewRecno           16
#define OP_PutIntKey          17
#define OP_PutStrKey          18
#define OP_Distinct           19
#define OP_Found              20
#define OP_NotFound           21
#define OP_IsUnique           22
#define OP_NotExists          23
#define OP_Delete             24
#define OP_Column             25
#define OP_KeyAsData          26
#define OP_Recno              27
#define OP_FullKey            28
#define OP_NullRow            29
#define OP_Last               30
#define OP_Rewind             31
#define OP_Next               32

#define OP_Destroy            33
#define OP_Clear              34
#define OP_CreateIndex        35
#define OP_CreateTable        36
#define OP_IntegrityCk        37

#define OP_IdxPut             38
#define OP_IdxDelete          39
#define OP_IdxRecno           40
#define OP_IdxGT              41
#define OP_IdxGE              42

#define OP_MemLoad            43
#define OP_MemStore           44
#define OP_MemIncr            45

#define OP_ListWrite          46
#define OP_ListRewind         47
#define OP_ListRead           48
#define OP_ListReset          49
#define OP_ListPush           50
#define OP_ListPop            51

#define OP_SortPut            52
#define OP_SortMakeRec        53
#define OP_SortMakeKey        54
#define OP_Sort               55
#define OP_SortNext           56
#define OP_SortCallback       57
#define OP_SortReset          58

#define OP_FileOpen           59
#define OP_FileRead           60
#define OP_FileColumn         61

#define OP_AggReset           62
#define OP_AggFocus           63
#define OP_AggNext            64
#define OP_AggSet             65
#define OP_AggGet             66
#define OP_AggFunc            67
#define OP_AggInit            68
#define OP_AggPush            69
#define OP_AggPop             70

#define OP_SetInsert          71
#define OP_SetFound           72
#define OP_SetNotFound        73
#define OP_SetFirst           74
#define OP_SetNext            75

#define OP_MakeRecord         76
#define OP_MakeKey            77
#define OP_MakeIdxKey         78
#define OP_IncrKey            79

#define OP_Goto               80
#define OP_If                 81
#define OP_IfNot              82
#define OP_Halt               83
#define OP_Gosub              84
#define OP_Return             85

#define OP_ColumnCount        86
#define OP_ColumnName         87
#define OP_Callback           88
#define OP_NullCallback       89

#define OP_Integer            90
#define OP_String             91
#define OP_Pop                92
#define OP_Dup                93
#define OP_Pull               94
#define OP_Push               95
#define OP_MustBeInt          96

#define OP_Add                97
#define OP_AddImm             98
#define OP_Subtract           99
#define OP_Multiply          100
#define OP_Divide            101
#define OP_Remainder         102
#define OP_BitAnd            103
#define OP_BitOr             104
#define OP_BitNot            105
#define OP_ShiftLeft         106
#define OP_ShiftRight        107
#define OP_AbsValue          108

/***** IMPORTANT NOTE: The code generator assumes that OP_XX+6==OP_StrXX *****/
#define OP_Eq                109
#define OP_Ne                110
#define OP_Lt                111
#define OP_Le                112
#define OP_Gt                113
#define OP_Ge                114
#define OP_StrEq             115
#define OP_StrNe             116
#define OP_StrLt             117
#define OP_StrLe             118
#define OP_StrGt             119
#define OP_StrGe             120
/***** IMPORTANT NOTE: the code generator assumes that OP_XX+6==OP_StrXX *****/

#define OP_IsNull            121
#define OP_NotNull           122
#define OP_Negative          123
#define OP_And               124
#define OP_Or                125
#define OP_Not               126
#define OP_Concat            127
#define OP_Noop              128
#define OP_Function          129

#define OP_MAX               129

/*
** Prototypes for the VDBE interface.  See comments on the implementation
** for a description of what each of these routines does.
*/
Vdbe *sqliteVdbeCreate(sqlite*);
void sqliteVdbeCreateCallback(Vdbe*, int*);
int sqliteVdbeAddOp(Vdbe*,int,int,int);
int sqliteVdbeAddOpList(Vdbe*, int nOp, VdbeOp const *aOp);
void sqliteVdbeChangeP1(Vdbe*, int addr, int P1);
void sqliteVdbeChangeP2(Vdbe*, int addr, int P2);
void sqliteVdbeChangeP3(Vdbe*, int addr, const char *zP1, int N);
void sqliteVdbeDequoteP3(Vdbe*, int addr);
int sqliteVdbeMakeLabel(Vdbe*);
void sqliteVdbeDelete(Vdbe*);
int sqliteVdbeOpcode(const char *zName);
int sqliteVdbeExec(Vdbe*,sqlite_callback,void*,char**,void*,
                   int(*)(void*,const char*,int));
int sqliteVdbeList(Vdbe*,sqlite_callback,void*,char**);
void sqliteVdbeResolveLabel(Vdbe*, int);
int sqliteVdbeCurrentAddr(Vdbe*);
void sqliteVdbeTrace(Vdbe*,FILE*);
void sqliteVdbeCompressSpace(Vdbe*,int);

#endif
