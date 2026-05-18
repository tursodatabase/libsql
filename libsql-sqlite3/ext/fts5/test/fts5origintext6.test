# 2014 Jan 08
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
#
# Tests focused on phrase queries.
#

source [file join [file dirname [info script]] fts5_common.tcl]
set testprefix fts5origintext6

# If SQLITE_ENABLE_FTS5 is not defined, omit this file.
ifcapable !fts5 {
  finish_test
  return
}

proc insert_data {tbl} {
  db eval "
  INSERT INTO $tbl (rowid, x, y) VALUES
    (1, 'ChH BDd HhG efc BjJ BGi GBG FdD','ciJ AFf ADf fBJ fhC GFI JEH fcA'),
    (2, 'deg AIG Fie jII cCd Hbf igF fEE','GeA Ija gJg EDc HFi DDI dCf aDd'),
    (3, 'IJC hga deC Jfa Aeg hfh CcH dfb','ajD hgC Jaf IfH CHe jIG AjD adF'),
    (4, 'FiH GJH IDA AiG bBc CGG Eih bIH','hHg JaH aii IHE Ggd gcH gji CGc'),
    (5, 'ceg CAd jFI GAB BGg EeC IdH acG','bBC eIG ifH eDE Adj bjb GCj ebA'),
    (6, 'Eac Fbh aFF Eea jeG EIj HCc JJH','hbd giE Gfe eiI dEF abE cJf cAb'),
    (7, 'dic hAc jEC AiG FEF jHc HiD HBI','aEd ebE Gfi AJG EBA faj GiG jjE'),
    (8, 'Fca iEe EgE jjJ gce ijf EGc EBi','gaI dhH bFg CFc HeC CjI Jfg ccH'),
    (9, 'cfd iaa HCf iHJ HjG ffh ABb ibi','CfG bia Dai eii Ejg Jeg fCg hDb'),
    (10, 'Jjf hJC IID HJj bGB EbJ cgg eBj','jci jhi JAF jIg Bei Bcd cAC AJd'),
    (11, 'egG Cdi bFf fEB hfH jDH jia Efd','FAd eCg fAi aiC baC eJG acF iGE'),
    (12, 'Ada Gde CJI ADG gJA Cbb ccF iAB','eAE ajC FBB ccd Jgh fJg ieg hGE'),
    (13, 'gBb fDG Jdd HdD fiJ Bed Cig iGg','heC FeI iaj gdg ebB giC HaD FIe'),
    (14, 'FiI iDd Ffe igI bgB EJf FHG hDF','cjC AeI abf Fah cbJ ffH jEb aib'),
    (15, 'jaF hBI jIH Gdh FEc Fij hgj jFh','dGA ADH feh AAI AfJ DbC gBi hGH'),
    (16, 'gjH BGg iGj aFE CAH edI idf HEH','hIf DDg fjB hGi cHF BCH FjG Bgd'),
    (17, 'iaI JGH hji gcj Dda eeG jDd CBi','cHg jeh caG gIc feF ihG hgJ Abj'),
    (18, 'jHI iDB eFf AiH EFB CDb IAj GbC','Ghe dEI gdI jai gib dAG BIa djb'),
    (19, 'abI fHG Ccf aAc FDa fiC agF bdB','afi hde IgE bGF cfg DHD diE aca'),
    (20, 'IFh eDJ jfh cDg dde JGJ GAf fIJ','IBa EfH faE aeI FIF baJ FGj EIH'),
    (21, 'Dee bFC bBA dEI CEj aJI ghA dCH','hBA ddA HJh dfj egI Dij dFE bGE'),
    (22, 'JFE BCj FgA afc Jda FGD iHJ HDh','eAI jHe BHD Gah bbD Bgj gbh eGB'),
    (23, 'edE CJE FjG aFI edA Cea FId iFe','ABG jcA ddj EEc Dcg hAI agA biA'),
    (24, 'AgE cfc eef cGh aFB DcH efJ hcH','eGF HaB diG fgi bdc iGJ FGJ fFB'),
    (25, 'aCa AgI GhC DDI hGJ Hgc Gcg bbG','iID Fga jHa jIj idj DFD bAC AFJ'),
    (26, 'gjC JGh Fge faa eCA iGG gHE Gai','bDi hFE BbI DHD Adb Fgi hCa Hij'),
    (27, 'Eji jEI jhF DFC afH cDh AGc dHA','IDe GcA ChF DIb Bif HfH agD DGh'),
    (28, 'gDD AEE Dfg ICf Cbi JdE jgH eEi','eEb dBG FDE jgf cAI FaJ jaA cDd'),
    (29, 'cbe Gec hgB Egi bca dHg bAJ jBf','EFB DgD GJc fDb EeE bBA GFC Hbe'),
    (30, 'Adc eHB afI hDc Bhh baE hcJ BBd','JAH deg bcF Dab Bgj Gbb JHi FIB'),
    (31, 'agF dIj AJJ Hfg cCG hED Igc fHC','JEf eia dHf Ggc Agj geD bEE Gei'),
    (32, 'DAd cCe cbJ FjG gJe gba dJA GCf','eAf hFc bGE ABI hHA IcE abF CCE'),
    (33, 'fFh jJe DhJ cDJ EBi AfD eFI IhG','fEG GCc Bjd EFF ggg CFe EHd ciB'),
    (34, 'Ejb BjI eAF HaD eEJ FaG Eda AHC','Iah hgD EJG fdD cIE Daj IFf eJh'),
    (35, 'aHG eCe FjA djJ dAJ jiJ IaE GGB','Acg iEF JfB FIC Eei ggj dic Iii'),
    (36, 'Fdb EDF GaF JjB ehH IgC hgi DCG','cag DHI Fah hAJ bbh egG Hia hgJ'),
    (37, 'HGg icC JEC AFJ Ddh dhi hfC Ich','fEg bED Bff hCJ EiA cIf bfG cGA'),
    (38, 'aEJ jGI BCi FaA ebA BHj cIJ GcC','dCH ADd bGB cFE AgF geD cbG jIc'),
    (39, 'JFB bBi heA BFA hgB Ahj EIE CgI','EIJ JFG FJE GeA Hdg HeH ACh GiA'),
    (40, 'agB DDC CED igC Dfc DhI eiC fHi','dAB dcg iJF cej Fcc cAc AfB Fdd'),
    (41, 'BdF DHj Ege hcG DEd eFa dCf gBb','FBG ChB cej iGd Hbh fCc Ibe Abh'),
    (42, 'Bgc DjI cbC jGD bdb hHB IJA IJH','heg cii abb IGf eDe hJc dii fcE'),
    (43, 'fhf ECa FiA aDh Jbf CiB Jhe ajD','GFE bIF aeD gDE BIE Jea DfC BEc'),
    (44, 'GjE dBj DbJ ICF aDh EEH Ejb jFb','dJj aEc IBg bEG Faf fjA hjf FAF'),
    (45, 'BfA efd IIJ AHG dDF eGg dIJ Gcb','Bfj jeb Ahc dAE ACH Dfb ieb dhC'),
    (46, 'Ibj ege geC dJh CIi hbD EAG fGA','DEb BFe Bjg FId Fhg HeF JAc BbE'),
    (47, 'dhB afC hgG bEJ aIe Cbe iEE JCD','bdg Ajc FGA jbh Jge iAj fIA jbE'),
    (48, 'egH iDi bfH iiI hGC jFF Hfd AHB','bjE Beb iCc haB gIH Dea bga dfd'),
    (49, 'jgf chc jGc Baj HBb jdE hgh heI','FFB aBd iEB EIG HGf Bbj EIi JbI'),
    (50, 'jhe EGi ajA fbH geh EHe FdC bij','jDE bBC gbH HeE dcH iBH IFE AHi'),
    (51, 'aCb JiD cgJ Bjj iAI Hbe IAF FhH','ijf bhE Jdf FED dCH bbG HcJ ebH');
  "
}

foreach_detail_mode $testprefix {
foreach external {0 1 2} {
  reset_db

  proc tokens {cmd} { 
    set ret [list]
    for {set iTok 0} {$iTok < [$cmd xInstCount]} {incr iTok} {
      set txt [$cmd xInstToken $iTok 0]
      set txt [string map [list "\0" "."] $txt]
      lappend ret $txt
    }
    set ret
  }
  sqlite3_fts5_create_function db tokens tokens
  sqlite3_fts5_register_origintext db

  set E(0) internal
  set E(1) external
  set E(2) contentless
  set e $E($external)

  db eval { CREATE TABLE ex(x, y) }
  switch -- $external {
    0 {
      do_execsql_test 1.$e.0 {
        CREATE VIRTUAL TABLE ft USING fts5(
            x, y, tokenize="origintext unicode61", tokendata=1, detail=%DETAIL%
        );
      }
    }

    1 {
      do_execsql_test 1.$e.0 {
        CREATE VIRTUAL TABLE ft USING fts5(
            x, y, tokenize="origintext unicode61", tokendata=1, detail=%DETAIL%,
            content=ex
        );
      }
    }

    2 {
      do_execsql_test 1.$e.0 {
        CREATE VIRTUAL TABLE ft USING fts5(
            x, y, tokenize="origintext unicode61", tokendata=1, detail=%DETAIL%,
            content=
        );
      }
    }
  }
  insert_data ex
  insert_data ft
  
  proc prefixquery {prefix bInst bYOnly} {
    set ret [list]
    db eval { SELECT rowid, x, y FROM ex ORDER BY rowid } {
      set row [list]
      set bSeen 0

      set T [concat $x $y]
      if {$bYOnly} { set T $y }

      foreach w $T {
        if {[string match -nocase $prefix $w]} {
          set bSeen 1
          if {$bInst} {
            set v [string tolower $w]
            if {$w != $v} { append v ".$w" }
            lappend row $v
          }
        }
      }
  
      if {$bSeen} {
        lappend ret $rowid
        lappend ret $row
      }
    }
  
    set ret
  }
  
  proc do_prefixquery_test {tn prefix} {
    set bInst [expr {$::e!="contentless" || "%DETAIL%"=="full"}]
    set expect [prefixquery $prefix $bInst 0]
    set expect2 [prefixquery $prefix $bInst 1]

    uplevel [list do_execsql_test $tn.1 "
        SELECT rowid, tokens(ft) FROM ft('$prefix')
    " $expect]
    uplevel [list do_execsql_test $tn.2 "
        SELECT rowid, tokens(ft) FROM ft(fts5_insttoken('$prefix'))
    " $expect]
    db eval { INSERT INTO ft(ft, rank) VALUES('insttoken', 1) }
    uplevel [list do_execsql_test $tn.3 "
        SELECT rowid, tokens(ft) FROM ft('$prefix')
    " $expect]
    db eval { INSERT INTO ft(ft, rank) VALUES('insttoken', 0) }

    if {"%DETAIL%"!="none"} {
      uplevel [list do_execsql_test $tn.4 "
          SELECT rowid, tokens(ft) FROM ft('y: $prefix')
      " $expect2]
      uplevel [list do_execsql_test $tn.5 "
          SELECT rowid, tokens(ft) FROM ft(fts5_insttoken('y: $prefix'))
      " $expect2]
      db eval { INSERT INTO ft(ft, rank) VALUES('insttoken', 1) }
      uplevel [list do_execsql_test $tn.6 "
          SELECT rowid, tokens(ft) FROM ft('y: $prefix')
      " $expect2]
      db eval { INSERT INTO ft(ft, rank) VALUES('insttoken', 0) }
    }
  }
  
  do_prefixquery_test 1.$e.1 a*
  do_prefixquery_test 1.$e.2 b*
  do_prefixquery_test 1.$e.3 c*
  do_prefixquery_test 1.$e.4 d*
  do_prefixquery_test 1.$e.5 e*
  do_prefixquery_test 1.$e.6 f*
  do_prefixquery_test 1.$e.7 g*
  do_prefixquery_test 1.$e.8 h*
  do_prefixquery_test 1.$e.9 i*
  do_prefixquery_test 1.$e.10 j*
}}



finish_test

