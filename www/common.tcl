# This file contains TCL procedures used to generate standard parts of
# web pages.
#

proc header {txt} {
  puts "<html><head><title>$txt</title></head>"
  puts \
{<body bgcolor="white" link="#50695f" vlink="#508896">
<table width="100%" border="0">
<tr><td valign="top"><img src="sqlite.gif"></td>
<td width="100%"></td>
<td valign="bottom">
<ul>
<li><a href="http://www.sqlite.org/cvstrac/tktnew">bugs</a></li>
<li><a href="changes.html">changes</a></li>
<li><a href="contrib">contrib</a></li>
<li><a href="download.html#cvs">cvs&nbsp;repository</a></li>
<li><a href="docs.html">documentation</a></li>
</ul>
</td>
<td width="10"></td>
<td valign="bottom">
<ul>
<li><a href="download.html">download</a></li>
<li><a href="faq.html">faq</a></li>
<li><a href="index.html">home</a></li>
<li><a href="support.html">mailing&nbsp;list</a></li>
<li><a href="index.html">news</a></li>
</ul>
</td>
<td width="10"></td>
<td valign="bottom">
<ul>
<li><a href="quickstart.html">quick&nbsp;start</a></li>
<li><a href="support.html">support</a></li>
<li><a href="lang.html">syntax</a></li>
<li><a href="http://www.sqlite.org/cvstrac/timeline">timeline</a></li>
<li><a href="http://www.sqlite.org/cvstrac/wiki">wiki</a></li>
</ul>
</td>
</tr></table>
<table width="100%">
<tr><td bgcolor="#80a796"></td></tr>
</table>}
}

proc footer {{rcsid {}}} {
  puts {
<table width="100%">
<tr><td bgcolor="#80a796"></td></tr>
</table>}
  set date [lrange $rcsid 3 4]
  if {$date!=""} {
    puts "<small><i>This page last modified on $date</i></small>"
  }
  puts {</body></html>}
}
