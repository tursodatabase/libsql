/*
** Copyright (c) 1999, 2000 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License as published by the Free Software Foundation; either
** version 2 of the License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
** 
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*************************************************************************
** Header file for the Virtual DataBase Engine (VDBE)
**
** This header defines the interface to the virtual database engine
** or VDBE.  The VDBE implements an abstract machine that runs a
** simple program to access and modify the underlying database.
**
** $Id: vdbe.h,v 1.18 2001/08/19 18:19:46 drh Exp $
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
};
typedef struct VdbeOp VdbeOp;

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
#define OP_OpenIdx             1
#define OP_OpenTbl             2
#define OP_Close               3
#define OP_Fetch               4
#define OP_Fcnt                5
#define OP_New                 6
#define OP_Put                 7
#define OP_Distinct            8
#define OP_Found               9
#define OP_NotFound           10
#define OP_Delete             11
#define OP_Field              12
#define OP_KeyAsData          13
#define OP_Key                14
#define OP_FullKey            15
#define OP_Rewind             16
#define OP_Next               17

#define OP_Destroy            18
#define OP_Reorganize         19

#define OP_BeginIdx           20
#define OP_NextIdx            21
#define OP_PutIdx             22
#define OP_DeleteIdx          23

#define OP_MemLoad            24
#define OP_MemStore           25

#define OP_ListOpen           26
#define OP_ListWrite          27
#define OP_ListRewind         28
#define OP_ListRead           29
#define OP_ListClose          30

#define OP_SortOpen           31
#define OP_SortPut            32
#define OP_SortMakeRec        33
#define OP_SortMakeKey        34
#define OP_Sort               35
#define OP_SortNext           36
#define OP_SortKey            37
#define OP_SortCallback       38
#define OP_SortClose          39

#define OP_FileOpen           40
#define OP_FileRead           41
#define OP_FileField          42
#define OP_FileClose          43

#define OP_AggReset           44
#define OP_AggFocus           45
#define OP_AggIncr            46
#define OP_AggNext            47
#define OP_AggSet             48
#define OP_AggGet             49

#define OP_SetInsert          50
#define OP_SetFound           51
#define OP_SetNotFound        52
#define OP_SetClear           53

#define OP_MakeRecord         54
#define OP_MakeKey            55

#define OP_Goto               56
#define OP_If                 57
#define OP_Halt               58

#define OP_ColumnCount        59
#define OP_ColumnName         60
#define OP_Callback           61

#define OP_Integer            62
#define OP_String             63
#define OP_Null               64
#define OP_Pop                65
#define OP_Dup                66
#define OP_Pull               67

#define OP_Add                68
#define OP_AddImm             69
#define OP_Subtract           70
#define OP_Multiply           71
#define OP_Divide             72
#define OP_Min                73
#define OP_Max                74
#define OP_Like               75
#define OP_Glob               76
#define OP_Eq                 77
#define OP_Ne                 78
#define OP_Lt                 79
#define OP_Le                 80
#define OP_Gt                 81
#define OP_Ge                 82
#define OP_IsNull             83
#define OP_NotNull            84
#define OP_Negative           85
#define OP_And                86
#define OP_Or                 87
#define OP_Not                88
#define OP_Concat             89
#define OP_Noop               90

#define OP_Strlen             91
#define OP_Substr             92

#define OP_MAX                93

/*
** Prototypes for the VDBE interface.  See comments on the implementation
** for a description of what each of these routines does.
*/
Vdbe *sqliteVdbeCreate(sqlite*);
int sqliteVdbeAddOp(Vdbe*,int,int,int,const char*,int);
int sqliteVdbeAddOpList(Vdbe*, int nOp, VdbeOp const *aOp);
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
