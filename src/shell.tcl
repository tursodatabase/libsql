#!/usr/bin/wish
#
# A GUI shell for SQLite
#

# The following code is slighly modified from the original.  See comments
# for the modifications...
############################################################################
# A console widget for Tcl/Tk.  Invoke console:create with a window name,
# a prompt string, and a title to get a new top-level window that allows 
# the user to enter tcl commands.  This is mainly useful for testing and
# debugging.
#
# Copyright (C) 1998, 1999 D. Richard Hipp
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Library General Public
# License as published by the Free Software Foundation; either
# version 2 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Library General Public License for more details.
# 
# You should have received a copy of the GNU Library General Public
# License along with this library; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place - Suite 330,
# Boston, MA  02111-1307, USA.
#
# Author contact information:
#   drh@acm.org
#   http://www.hwaci.com/drh/

# Create a console widget named $w.  The prompt string is $prompt.
# The title at the top of the window is $title
#
proc console:create {w prompt title} {
  upvar #0 $w.t v
  if {[winfo exists $w]} {destroy $w}
  if {[info exists v]} {unset v}
  toplevel $w
  wm title $w $title
  wm iconname $w $title
  frame $w.mb -bd 2 -relief raised
  pack $w.mb -side top -fill x
  menubutton $w.mb.file -text File -menu $w.mb.file.m
  menubutton $w.mb.edit -text Edit -menu $w.mb.edit.m
  pack $w.mb.file $w.mb.edit -side left -padx 8 -pady 1
  set m [menu $w.mb.file.m]
  # $m add command -label {Source...} -command "console:SourceFile $w.t"
  # $m add command -label {Save As...} -command "console:SaveFile $w.t"
  # $m add separator
  $m add command -label {Close} -command "destroy $w"
  $m add command -label {Exit} -command exit
  console:create_child $w $prompt $w.mb.edit.m
}

# This routine creates a console as a child window within a larger
# window.  It also creates an edit menu named "$editmenu" if $editmenu!="".
# The calling function is responsible for posting the edit menu.
#
proc console:create_child {w prompt editmenu} {
  upvar #0 $w.t v
  if {$editmenu!=""} {
    set m [menu $editmenu]
    $m add command -label Cut -command "console:Cut $w.t"
    $m add command -label Copy -command "console:Copy $w.t"
    $m add command -label Paste -command "console:Paste $w.t"
    $m add command -label {Clear Screen} -command "console:Clear $w.t"
    $m add separator
    $m add command -label {Source...} -command "console:SourceFile $w.t"
    $m add command -label {Save As...} -command "console:SaveFile $w.t"
    catch {$editmenu config -postcommand "console:EnableEditMenu $w"}
  }
  scrollbar $w.sb -orient vertical -command "$w.t yview"
  pack $w.sb -side right -fill y
  text $w.t -font fixed -yscrollcommand "$w.sb set"
  pack $w.t -side right -fill both -expand 1
  bindtags $w.t Console
  set v(editmenu) $editmenu
  set v(text) $w.t
  set v(history) 0
  set v(historycnt) 0
  set v(current) -1
  set v(prompt) $prompt
  set v(prior) {}
  set v(plength) [string length $v(prompt)]
  set v(x) 0
  set v(y) 0
  $w.t mark set insert end
  $w.t tag config ok -foreground blue
  $w.t tag config err -foreground red
  $w.t insert end $v(prompt)
  $w.t mark set out 1.0
  catch {rename puts console:oldputs$w}
  proc puts args [format {
    if {![winfo exists %s]} {
      rename puts {}
      rename console:oldputs%s puts
      return [uplevel #0 puts $args]
    }
    switch -glob -- "[llength $args] $args" {
      {1 *} {
         set msg [lindex $args 0]\n
         set tag ok
      }
      {2 stdout *} {
         set msg [lindex $args 1]\n
         set tag ok
      }
      {2 stderr *} {
         set msg [lindex $args 1]\n
         set tag err
      }
      {2 -nonewline *} {
         set msg [lindex $args 1]
         set tag ok
      }
      {3 -nonewline stdout *} {
         set msg [lindex $args 2]
         set tag ok
      }
      {3 -nonewline stderr *} {
         set msg [lindex $args 2]
         set tag err
      }
      default {
        uplevel #0 console:oldputs%s $args
        return
      }
    }
    console:Puts %s $msg $tag
  } $w $w $w $w.t]
  after idle "focus $w.t"
}

bind Console <1> {console:Button1 %W %x %y}
bind Console <B1-Motion> {console:B1Motion %W %x %y}
bind Console <B1-Leave> {console:B1Leave %W %x %y}
bind Console <B1-Enter> {console:cancelMotor %W}
bind Console <ButtonRelease-1> {console:cancelMotor %W}
bind Console <KeyPress> {console:Insert %W %A}
bind Console <Left> {console:Left %W}
bind Console <Control-b> {console:Left %W}
bind Console <Right> {console:Right %W}
bind Console <Control-f> {console:Right %W}
bind Console <BackSpace> {console:Backspace %W}
bind Console <Control-h> {console:Backspace %W}
bind Console <Delete> {console:Delete %W}
bind Console <Control-d> {console:Delete %W}
bind Console <Home> {console:Home %W}
bind Console <Control-a> {console:Home %W}
bind Console <End> {console:End %W}
bind Console <Control-e> {console:End %W}
bind Console <Return> {console:Enter %W}
bind Console <KP_Enter> {console:Enter %W}
bind Console <Up> {console:Prior %W}
bind Console <Control-p> {console:Prior %W}
bind Console <Down> {console:Next %W}
bind Console <Control-n> {console:Next %W}
bind Console <Control-k> {console:EraseEOL %W}
bind Console <<Cut>> {console:Cut %W}
bind Console <<Copy>> {console:Copy %W}
bind Console <<Paste>> {console:Paste %W}
bind Console <<Clear>> {console:Clear %W}

# Insert test at the "out" mark.  The "out" mark is always
# before the input line.  New text appears on the line prior
# to the current input line.
#
proc console:Puts {w t tag} {
  set nc [string length $t]
  set endc [string index $t [expr $nc-1]]
  if {$endc=="\n"} {
    if {[$w index out]<[$w index {insert linestart}]} {
      $w insert out [string range $t 0 [expr $nc-2]] $tag
      $w mark set out {out linestart +1 lines}
    } else {
      $w insert out $t $tag
    }
  } else {
    if {[$w index out]<[$w index {insert linestart}]} {
      $w insert out $t $tag
    } else {
      $w insert out $t\n $tag
      $w mark set out {out -1 char}
    }
  }
  $w yview insert
}

# Insert a single character at the insertion cursor
#
proc console:Insert {w a} {
  $w insert insert $a
  $w yview insert
}

# Move the cursor one character to the left
#
proc console:Left {w} {
  upvar #0 $w v
  scan [$w index insert] %d.%d row col
  if {$col>$v(plength)} {
    $w mark set insert "insert -1c"
  }
}

# Erase the character to the left of the cursor
#
proc console:Backspace {w} {
  upvar #0 $w v
  scan [$w index insert] %d.%d row col
  if {$col>$v(plength)} {
    $w delete {insert -1c}
  }
}

# Erase to the end of the line
#
proc console:EraseEOL {w} {
  upvar #0 $w v
  scan [$w index insert] %d.%d row col
  if {$col>=$v(plength)} {
    $w delete insert {insert lineend}
  }
}

# Move the cursor one character to the right
#
proc console:Right {w} {
  $w mark set insert "insert +1c"
}

# Erase the character to the right of the cursor
#
proc console:Delete w {
  $w delete insert
}

# Move the cursor to the beginning of the current line
#
proc console:Home w {
  upvar #0 $w v
  scan [$w index insert] %d.%d row col
  $w mark set insert $row.$v(plength)
}

# Move the cursor to the end of the current line
#
proc console:End w {
  $w mark set insert {insert lineend}
}

# Called when "Enter" is pressed.  Do something with the line
# of text that was entered.
#
proc console:Enter w {
  upvar #0 $w v
  scan [$w index insert] %d.%d row col
  set start $row.$v(plength)
  set line [$w get $start "$start lineend"]
  if {$v(historycnt)>0} {
    set last [lindex $v(history) [expr $v(historycnt)-1]]
    if {[string compare $last $line]} {
      lappend v(history) $line
      incr v(historycnt)
    }
  } else {
    set v(history) [list $line]
    set v(historycnt) 1
  }
  set v(current) $v(historycnt)
  $w insert end \n
  $w mark set out end
  if {$v(prior)==""} {
    set cmd $line
  } else {
    set cmd $v(prior)\n$line
  }
##### Original
# if {[info complete $cmd]} {    }
#   set rc [catch {uplevel #0 $cmd} res]
##### New
  global DB 
  if {[$DB complete $cmd]} {
    set CODE {}
    set rc [catch {$DB eval $cmd RESULT $CODE}]
##### End Of Changes
    if {![winfo exists $w]} return
    if {$rc} {
      $w insert end $res\n err
    } elseif {[string length $res]>0} {
      $w insert end $res\n ok
    }
    set v(prior) {}
    $w insert end $v(prompt)
  } else {
    set v(prior) $cmd
    regsub -all {[^ ]} $v(prompt) . x
    $w insert end $x
  }
  $w mark set insert end
  $w mark set out {insert linestart}
  $w yview insert
}

# Change the line to the previous line
#
proc console:Prior w {
  upvar #0 $w v
  if {$v(current)<=0} return
  incr v(current) -1
  set line [lindex $v(history) $v(current)]
  console:SetLine $w $line
}

# Change the line to the next line
#
proc console:Next w {
  upvar #0 $w v
  if {$v(current)>=$v(historycnt)} return
  incr v(current) 1
  set line [lindex $v(history) $v(current)]
  console:SetLine $w $line
}

# Change the contents of the entry line
#
proc console:SetLine {w line} {
  upvar #0 $w v
  scan [$w index insert] %d.%d row col
  set start $row.$v(plength)
  $w delete $start end
  $w insert end $line
  $w mark set insert end
  $w yview insert
}

# Called when the mouse button is pressed at position $x,$y on
# the console widget.
#
proc console:Button1 {w x y} {
  global tkPriv
  upvar #0 $w v
  set v(mouseMoved) 0
  set v(pressX) $x
  set p [console:nearestBoundry $w $x $y]
  scan [$w index insert] %d.%d ix iy
  scan $p %d.%d px py
  if {$px==$ix} {
    $w mark set insert $p
  }
  $w mark set anchor $p
  focus $w
}

# Find the boundry between characters that is nearest
# to $x,$y
#
proc console:nearestBoundry {w x y} {
  set p [$w index @$x,$y]
  set bb [$w bbox $p]
  if {![string compare $bb ""]} {return $p}
  if {($x-[lindex $bb 0])<([lindex $bb 2]/2)} {return $p}
  $w index "$p + 1 char"
}

# This routine extends the selection to the point specified by $x,$y
#
proc console:SelectTo {w x y} {
  upvar #0 $w v
  set cur [console:nearestBoundry $w $x $y]
  if {[catch {$w index anchor}]} {
    $w mark set anchor $cur
  }
  set anchor [$w index anchor]
  if {[$w compare $cur != $anchor] || (abs($v(pressX) - $x) >= 3)} {
    if {$v(mouseMoved)==0} {
      $w tag remove sel 0.0 end
    }
    set v(mouseMoved) 1
  }
  if {[$w compare $cur < anchor]} {
    set first $cur
    set last anchor
  } else {
    set first anchor
    set last $cur
  }
  if {$v(mouseMoved)} {
    $w tag remove sel 0.0 $first
    $w tag add sel $first $last
    $w tag remove sel $last end
    update idletasks
  }
}

# Called whenever the mouse moves while button-1 is held down.
#
proc console:B1Motion {w x y} {
  upvar #0 $w v
  set v(y) $y
  set v(x) $x
  console:SelectTo $w $x $y
}

# Called whenever the mouse leaves the boundries of the widget
# while button 1 is held down.
#
proc console:B1Leave {w x y} {
  upvar #0 $w v
  set v(y) $y
  set v(x) $x
  console:motor $w
}

# This routine is called to automatically scroll the window when
# the mouse drags offscreen.
#
proc console:motor w {
  upvar #0 $w v
  if {![winfo exists $w]} return
  if {$v(y)>=[winfo height $w]} {
    $w yview scroll 1 units
  } elseif {$v(y)<0} {
    $w yview scroll -1 units
  } else {
    return
  }
  console:SelectTo $w $v(x) $v(y)
  set v(timer) [after 50 console:motor $w]
}

# This routine cancels the scrolling motor if it is active
#
proc console:cancelMotor w {
  upvar #0 $w v
  catch {after cancel $v(timer)}
  catch {unset v(timer)}
}

# Do a Copy operation on the stuff currently selected.
#
proc console:Copy w {
  if {![catch {set text [$w get sel.first sel.last]}]} {
     clipboard clear -displayof $w
     clipboard append -displayof $w $text
  }
}

# Return 1 if the selection exists and is contained
# entirely on the input line.  Return 2 if the selection
# exists but is not entirely on the input line.  Return 0
# if the selection does not exist.
#
proc console:canCut w {
  set r [catch {
    scan [$w index sel.first] %d.%d s1x s1y
    scan [$w index sel.last] %d.%d s2x s2y
    scan [$w index insert] %d.%d ix iy
  }]
  if {$r==1} {return 0}
  if {$s1x==$ix && $s2x==$ix} {return 1}
  return 2
}

# Do a Cut operation if possible.  Cuts are only allowed
# if the current selection is entirely contained on the
# current input line.
#
proc console:Cut w {
  if {[console:canCut $w]==1} {
    console:Copy $w
    $w delete sel.first sel.last
  }
}

# Do a paste opeation.
#
proc console:Paste w {
  if {[console:canCut $w]==1} {
    $w delete sel.first sel.last
  }
  if {[catch {selection get -displayof $w -selection CLIPBOARD} topaste]} {
    return
  }
  set prior 0
  foreach line [split $topaste \n] {
    if {$prior} {
      console:Enter $w
      update
    }
    set prior 1
    $w insert insert $line
  }
}

# Enable or disable entries in the Edit menu
#
proc console:EnableEditMenu w {
  upvar #0 $w.t v
  set m $v(editmenu)
  if {$m=="" || ![winfo exists $m]} return
  switch [console:canCut $w.t] {
    0 {
      $m entryconf Copy -state disabled
      $m entryconf Cut -state disabled
    }
    1 {
      $m entryconf Copy -state normal
      $m entryconf Cut -state normal
    }
    2 {
      $m entryconf Copy -state normal
      $m entryconf Cut -state disabled
    }
  }
}

# Prompt for the user to select an input file, the "source" that file.
#
proc console:SourceFile w {
  set types {
    {{TCL Scripts}  {.tcl}}
    {{All Files}    *}
  }
  set f [tk_getOpenFile -filetypes $types -title "TCL Script To Source..."]
  if {$f!=""} {
    uplevel #0 source $f
  }
}

# Prompt the user for the name of a writable file.  Then write the
# entire contents of the console screen to that file.
#
proc console:SaveFile w {
  set types {
    {{Text Files}  {.txt}}
    {{All Files}    *}
  }
  set f [tk_getSaveFile -filetypes $types -title "Write Screen To..."]
  if {$f!=""} {
    if {[catch {open $f w} fd]} {
      tk_messageBox -type ok -icon error -message $fd
    } else {
      puts $fd [string trimright [$w get 1.0 end] \n]
      close $fd
    }
  }
}

# Erase everything from the console above the insertion line.
#
proc console:Clear w {
  $w delete 1.0 {insert linestart}
}

# Start the console
#
# console:create {.@console} {% } {Tcl/Tk Console}
###############################################################################


if {[info command sqlite]==""} {
  load ./tclsqlite.so sqlite
}



proc set_title {title} {
  if {$title==""} {
    set main SQLite
  } else {
    set main "SQLite - $title"
  }
  wm title . $main
  wm iconname . SQLite
}
set_title {}

frame .mb -bd 1 -relief raised
pack .mb -side top -fill x
menubutton .mb.file -text File -underline 0 -menu .mb.file.m
pack .mb.file -side left -padx 5
set m [menu .mb.file.m]
$m add separator
$m add command -label Exit -command exit
menubutton .mb.edit -text Edit -underline 0 -menu .mb.edit.m
pack .mb.edit -side left -padx 5
#menu .mb.edit.m

frame .f
pack .f -side top -fill both -expand 1
console:create_child .f {sqlite> } .mb.edit.m
