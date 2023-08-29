/*
** SCRIPT_MODULE_NAME:      fts5-sanity-checks
** xREQUIRED_PROPERTIES:     FTS5
**
*/

--testcase 1.0
CREATE VIRTUAL TABLE email USING fts5(sender, title, body);
insert into email values('fred','Help!','Dear Sir...');
insert into email values('barney','Assistance','Dear Madam...');
select * from email where email match 'assistance';
--result barney Assistance {Dear Madam...}
