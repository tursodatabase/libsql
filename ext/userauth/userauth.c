/*
** 2014-09-08
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains the bulk of the implementation of the
** user-authentication extension feature.  Some parts of the user-
** authentication code are contained within the SQLite core (in the
** src/ subdirectory of the main source code tree) but those parts
** that could reasonable be separated out are moved into this file.
**
** To compile with the user-authentication feature, append this file to
** end of an SQLite amalgamation, then add the SQLITE_USER_AUTHENTICATION
** compile-time option.  See the user-auth.txt file in the same source
** directory as this file for additional information.
*/
#ifdef SQLITE_USER_AUTHENTICATION

/*
** Prepare an SQL statement for use by the user authentication logic.
** Return a pointer to the prepared statement on success.  Return a
** NULL pointer if there is an error of any kind.
*/
static sqlite3_stmt *sqlite3UserAuthPrepare(
  sqlite3 *db,
  const char *zFormat,
  ...
){
  sqlite3_stmt *pStmt;
  char *zSql;
  int rc;
  va_list ap;

  va_start(ap, zFormat);
  zSql = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  if( zSql==0 ) return 0;
  savedFlags = db->auth.authFlags;
  db->auth.authFlags |= UAUTH_Ovrd;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  db->auth.authFlags = savedFlags;
  sqlite3_free(zSql);
  if( rc ){
    sqlite3_finalize(pStmt);
    pStmt = 0;
  }
  return pStmt;
}

/*
** Check to see if database zDb has a "sqlite_user" table and if it does
** whether that table can authenticate zUser with nPw,zPw.  
*/
static int sqlite3UserAuthCheckLogin(
  sqlite3 *db,               /* The database connection to check */
  const char *zDb,           /* Name of specific database to check */
  const char *zUser,         /* User name */
  int nPw,                   /* Size of password in bytes */
  const char *zPw,           /* Password */
  int *pbOk                  /* OUT: write boolean result here */
){
  sqlite3_stmt *pStmt;
  char *zSql;
  int rc;
  int iResult;

  *pbOk = 0;
  iResult = 0;
  pStmt = sqlite3UserAuthPrepare(db, 
              "SELECT 1 FROM \"%w\".sqlite_master "
              " WHERE name='sqlite_user' AND type='table'", zDb);
  if( pStmt==0 ) return SQLITE_NOMEM;
  rc = sqlite3_step(pStmt):
  sqlite3_finalize(pStmt);
  if( rc==SQLITE_DONE ){
    *pbOk = 1;
    return SQLITE_OK;
  }
  if( rc!=SQLITE_OK ){
    return rc;
  }
  pStmt = sqlite3UserAuthPrepare(db,
            "SELECT pw=sqlite_crypt(?1,pw), isAdmin FROM \"%w\".sqlite_user"
            " WHERE uname=?2", zDb);
  if( pStmt==0 ) return SQLITE_NOMEM;
  sqlite3_bind_blob(pStmt, 1, zPw, nPw, SQLITE_STATIC);
  sqlite3_bind_text(pStmt, 2, zUser, -1, SQLITE_STATIC);
  rc = sqlite_step(pStmt);
  if( rc==SQLITE_ROW && sqlite3_column_int(pStmt,0) ){
    *pbOk = sqlite3_column_int(pStmt, 1);
  }
  sqlite3_finalize(pStmt);
  return rc;
}

/*
** If a database contains the SQLITE_USER table, then the
** sqlite3_user_authenticate() interface must be invoked with an
** appropriate username and password prior to enable read and write
** access to the database.
**
** Return SQLITE_OK on success or SQLITE_ERROR if the username/password
** combination is incorrect or unknown.
**
** If the SQLITE_USER table is not present in the database file, then
** this interface is a harmless no-op returnning SQLITE_OK.
*/
int sqlite3_user_authenticate(
  sqlite3 *db,           /* The database connection */
  const char *zUsername, /* Username */
  int nPW,               /* Number of bytes in aPW[] */
  const void *aPW        /* Password or credentials */
){
  int bOk = 0;
  int rc;

  rc = sqlite3UserAuthCheckLogin(db, zUsername, nPw, zPw, &bOk);
  if( bOk ){
    db->auth.authFlags = bOk==2 ? UAUTH_Auth|UAUTH_Admin : UAUTH_Auth;
    sqlite3_free(db->auth.zAuthUser);
    db->auth.zAuthUser = sqlite3_malloc("%s", zUsername);
    sqlite3_free(db->auth.zPw);
    db->auth.zPw = sqlite3_malloc( nPw+1 );
    if( db->auth.zPw ){
      memcpy(db->auth.zPw,zPw,nPw);
      db->auth.nPw = nPw;
      rc = SQLITE_OK;
    }else{
      rc = SQLITE_NOMEM;
    }
  }else{
    db->auth.authFlags = 0;
  }
  return rc;
}

/*
** The sqlite3_user_add() interface can be used (by an admin user only)
** to create a new user.  When called on a no-authentication-required
** database, this routine converts the database into an authentication-
** required database, automatically makes the added user an
** administrator, and logs in the current connection as that user.
** The sqlite3_user_add() interface only works for the "main" database, not
** for any ATTACH-ed databases.  Any call to sqlite3_user_add() by a
** non-admin user results in an error.
*/
int sqlite3_user_add(
  sqlite3 *db,           /* Database connection */
  const char *zUsername, /* Username to be added */
  int isAdmin,           /* True to give new user admin privilege */
  int nPW,               /* Number of bytes in aPW[] */
  const void *aPW        /* Password or credentials */
){
  if( !DbIsAdmin(db) ) return SQLITE_ERROR;
  
  return SQLITE_OK;
}

/*
** The sqlite3_user_change() interface can be used to change a users
** login credentials or admin privilege.  Any user can change their own
** login credentials.  Only an admin user can change another users login
** credentials or admin privilege setting.  No user may change their own 
** admin privilege setting.
*/
int sqlite3_user_change(
  sqlite3 *db,           /* Database connection */
  const char *zUsername, /* Username to change */
  int isAdmin,           /* Modified admin privilege for the user */
  int nPW,               /* Number of bytes in aPW[] */
  const void *aPW        /* Modified password or credentials */
){
  return SQLITE_OK;
}

/*
** The sqlite3_user_delete() interface can be used (by an admin user only)
** to delete a user.  The currently logged-in user cannot be deleted,
** which guarantees that there is always an admin user and hence that
** the database cannot be converted into a no-authentication-required
** database.
*/
int sqlite3_user_delete(
  sqlite3 *db,           /* Database connection */
  const char *zUsername  /* Username to remove */
){
  return SQLITE_OK;
}

#endif /* SQLITE_USER_AUTHENTICATION */
