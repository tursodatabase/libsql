/*
  2022-07-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file is the tail end of the sqlite3-api.js constellation,
  intended to be appended after all other files so that it can clean
  up any global systems temporarily used for setting up the API's
  various subsystems.
*/
'use strict';
self.sqlite3.postInit.forEach(
  self.importScripts/*global is a Worker*/
    ? function(f){
      /** We try/catch/report for the sake of failures which happen in
          a Worker, as those exceptions can otherwise get completely
          swallowed, leading to confusing downstream errors which have
          nothing to do with this failure. */
      try{ f(self, self.sqlite3) }
      catch(e){
        console.error("Error in postInit() function:",e);
        throw e;
      }
    }
  : (f)=>f(self, self.sqlite3)
);
delete self.sqlite3.postInit;
if(self.location && +self.location.port > 1024){
  console.warn("Installing sqlite3 bits as global S for dev-testing purposes.");
  self.S = self.sqlite3;
}
/* Clean up temporary global-scope references to our APIs... */
self.sqlite3.config.Module.sqlite3 = self.sqlite3
/* ^^^^ Currently needed by test code and Worker API setup */;
delete self.sqlite3.capi.util /* arguable, but these are (currently) internal-use APIs */;
delete self.sqlite3 /* clean up our global-scope reference */;
//console.warn("Module.sqlite3 =",Module.sqlite3);
