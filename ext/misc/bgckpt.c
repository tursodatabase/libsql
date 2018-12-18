/*
** 2017-10-11
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
*/

#if !defined(SQLITE_TEST) || defined(SQLITE_OS_UNIX)

#include "sqlite3.h"
#include <string.h>
#include <pthread.h>

/*
** API declarations.
*/
typedef struct Checkpointer Checkpointer;
int sqlite3_bgckpt_create(const char *zFilename, Checkpointer **pp);
int sqlite3_bgckpt_checkpoint(Checkpointer *p, int bBlock);
void sqlite3_bgckpt_destroy(Checkpointer *p);


struct Checkpointer {
  sqlite3 *db;                    /* Database handle */

  pthread_t thread;               /* Background thread */
  pthread_mutex_t mutex;
  pthread_cond_t cond;

  int rc;                         /* Error from "PRAGMA wal_checkpoint" */
  int bCkpt;                      /* True if checkpoint requested */
  int bExit;                      /* True if exit requested */
};

static void *bgckptThreadMain(void *pCtx){
  int rc = SQLITE_OK;
  Checkpointer *p = (Checkpointer*)pCtx;

  while( rc==SQLITE_OK ){
    int bExit;

    pthread_mutex_lock(&p->mutex);
    if( p->bCkpt==0 && p->bExit==0 ){
      pthread_cond_wait(&p->cond, &p->mutex);
    }
    p->bCkpt = 0;
    bExit = p->bExit;
    pthread_mutex_unlock(&p->mutex);

    if( bExit ) break;
    rc = sqlite3_exec(p->db, "PRAGMA wal_checkpoint", 0, 0, 0);
    if( rc==SQLITE_BUSY ){
      rc = SQLITE_OK;
    }
  }

  pthread_mutex_lock(&p->mutex);
  p->rc = rc;
  pthread_mutex_unlock(&p->mutex);
  return 0;
}

void sqlite3_bgckpt_destroy(Checkpointer *p){
  if( p ){
    void *ret = 0;

    /* Signal the background thread to exit */
    pthread_mutex_lock(&p->mutex);
    p->bExit = 1;
    pthread_cond_broadcast(&p->cond);
    pthread_mutex_unlock(&p->mutex);

    pthread_join(p->thread, &ret);
    sqlite3_close(p->db);
    sqlite3_free(p);
  }
}


int sqlite3_bgckpt_create(const char *zFilename, Checkpointer **pp){
  Checkpointer *pNew = 0;
  int rc;

  pNew = (Checkpointer*)sqlite3_malloc(sizeof(Checkpointer));
  if( pNew==0 ){
    rc = SQLITE_NOMEM;
  }else{
    memset(pNew, 0, sizeof(Checkpointer));
    rc = sqlite3_open(zFilename, &pNew->db);
  }

  if( rc==SQLITE_OK ){
    pthread_mutex_init(&pNew->mutex, 0);
    pthread_cond_init(&pNew->cond, 0);
    pthread_create(&pNew->thread, 0, bgckptThreadMain, (void*)pNew);
  }

  if( rc!=SQLITE_OK ){
    sqlite3_bgckpt_destroy(pNew);
    pNew = 0;
  }
  *pp = pNew;
  return rc;
}

int sqlite3_bgckpt_checkpoint(Checkpointer *p, int bBlock){
  int rc;
  pthread_mutex_lock(&p->mutex);
  rc = p->rc;
  if( rc==SQLITE_OK ){
    p->bCkpt = 1;
    pthread_cond_broadcast(&p->cond);
  }
  pthread_mutex_unlock(&p->mutex);
  return rc;
}

#ifdef SQLITE_TEST

#if defined(INCLUDE_SQLITE_TCL_H)
#  include "sqlite_tcl.h"
#else
#  include "tcl.h"
#  ifndef SQLITE_TCLAPI
#    define SQLITE_TCLAPI
#  endif
#endif

const char *sqlite3ErrName(int rc);

static void SQLITE_TCLAPI bgckpt_del(void * clientData){
  Checkpointer *pCkpt = (Checkpointer*)clientData;
  sqlite3_bgckpt_destroy(pCkpt);
}

/*
** Tclcmd: $ckpt SUBCMD ...
*/
static int SQLITE_TCLAPI bgckpt_obj_cmd(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  Checkpointer *pCkpt = (Checkpointer*)clientData;
  const char *aCmd[] = { "checkpoint", "destroy", 0 };
  int iCmd;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUBCMD ...");
    return TCL_ERROR;
  }

  if( Tcl_GetIndexFromObj(interp, objv[1], aCmd, "sub-command", 0, &iCmd) ){
    return TCL_ERROR;
  }

  switch( iCmd ){
    case 0: {
      int rc;
      int bBlock = 0;

      if( objc>3 ){
        Tcl_WrongNumArgs(interp, 2, objv, "?BLOCKING?");
        return TCL_ERROR;
      }
      if( objc==3 && Tcl_GetBooleanFromObj(interp, objv[2], &bBlock) ){
        return TCL_ERROR;
      }

      rc = sqlite3_bgckpt_checkpoint(pCkpt, bBlock);
      if( rc!=SQLITE_OK ){
        Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(rc), -1));
        return TCL_ERROR;
      }
      break;
    }

    case 1: {
      Tcl_DeleteCommand(interp, Tcl_GetString(objv[0]));
      break;
    }
  }

  return TCL_OK;
}

/*
** Tclcmd: bgckpt CMDNAME FILENAME
*/
static int SQLITE_TCLAPI bgckpt_cmd(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  const char *zCmd;
  const char *zFilename;
  int rc;
  Checkpointer *pCkpt;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "CMDNAME FILENAME");
    return TCL_ERROR;
  }
  zCmd = Tcl_GetString(objv[1]);
  zFilename = Tcl_GetString(objv[2]);

  rc = sqlite3_bgckpt_create(zFilename, &pCkpt);
  if( rc!=SQLITE_OK ){
    Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(rc), -1));
    return TCL_ERROR;
  }

  Tcl_CreateObjCommand(interp, zCmd, bgckpt_obj_cmd, (void*)pCkpt, bgckpt_del);
  Tcl_SetObjResult(interp, objv[1]);
  return TCL_OK;
}

int Bgckpt_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "bgckpt", bgckpt_cmd, 0, 0);
  return TCL_OK;
}
#endif   /* SQLITE_TEST */

#else
#if defined(INCLUDE_SQLITE_TCL_H)
#  include "sqlite_tcl.h"
#else
#  include "tcl.h"
#  ifndef SQLITE_TCLAPI
#    define SQLITE_TCLAPI
#  endif
#endif
int Bgckpt_Init(Tcl_Interp *interp){ return TCL_OK; }
#endif

