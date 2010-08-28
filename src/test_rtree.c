/*
** 2010 August 28
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Code for testing all sorts of SQLite interfaces. This code
** is not included in the SQLite library. 
*/
#include "sqlite3rtree.h"
#include <sqlite3.h>
#include <assert.h>
#include "tcl.h"

typedef struct Cube Cube;
struct Cube {
  double x;
  double y;
  double z;
  double width;
  double height;
  double depth;
};

static void cube_context_free(void *p){
  sqlite3_free(p);
}

static int gHere = 42;

/*
** Implementation of a simple r-tree geom callback to test for intersection
** of r-tree rows with a "cube" shape. Cubes are defined by six scalar
** coordinates as follows:
**
**   cube(x, y, z, width, height, depth)
**
** The width, height and depth parameters must all be greater than zero.
*/
static int cube_geom(
  RtreeGeometry *p,
  int nCoord, 
  double *aCoord, 
  int *piRes
){
  Cube *pCube = (Cube *)p->pUser;

  assert( p->pContext==(void *)&gHere );

  if( pCube==0 ){
    if( p->nParam!=6 || nCoord!=6
     || p->aParam[3]<=0.0 || p->aParam[4]<=0.0 || p->aParam[5]<=0.0
    ){
      return SQLITE_ERROR;
    }
    pCube = (Cube *)sqlite3_malloc(sizeof(Cube));
    if( !pCube ){
      return SQLITE_NOMEM;
    }
    pCube->x = p->aParam[0];
    pCube->y = p->aParam[1];
    pCube->z = p->aParam[2];
    pCube->width = p->aParam[3];
    pCube->height = p->aParam[4];
    pCube->depth = p->aParam[5];

    p->pUser = (void *)pCube;
    p->xDelUser = cube_context_free;
  }

  assert( nCoord==6 );
  *piRes = 0;
  if( aCoord[0]<=(pCube->x+pCube->width)
   && aCoord[1]>=pCube->x
   && aCoord[2]<=(pCube->y+pCube->height)
   && aCoord[3]>=pCube->y
   && aCoord[4]<=(pCube->z+pCube->depth)
   && aCoord[5]>=pCube->z
  ){
    *piRes = 1;
  }

  return SQLITE_OK;
}

static int register_cube_geom(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
#ifdef SQLITE_ENABLE_RTREE
  extern int getDbPointer(Tcl_Interp*, const char*, sqlite3**);
  sqlite3 *db;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ) return TCL_ERROR;
  sqlite3_rtree_geometry_callback(db, "cube", cube_geom, (void *)&gHere);
#endif
  return TCL_OK;
}

int Sqlitetestrtree_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "register_cube_geom", register_cube_geom, 0, 0);
  return TCL_OK;
}

