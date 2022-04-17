# Introduction to the TCL Extension for the SQLite Extensible Shell #

This article introduces an extension,
written to be hosted by the SQLite Extensible Shell,
which adds [Tcl](https://www.tcl.tk/about/index.html) features to it.

## Motivation ##

An understanding of the motivation for this feature may prove useful
for putting what follows into context.
Over its 2+ decade existence, the sqlite3 CLI shell has evolved,
from its original utility as a test vehicle for the SQLite library,
to a tool used by many people who want to use SQLite databases
without writing a specialized application. While the shell exposes
many of the library features, its original design was best suited
for performing short, fixed sequences of DB operations.

Effecting more complex, algorithmically determined operations
can be difficult,
usually requiring use of a general purpose scripting tool
to drive the sqlite3 shell.
This can work well where the task can be decomposed into
"Prepare a sequence of elementary DB operations,"
followed by
"then feed the sequence into sqlite3 as its command/SQL input."
However, where the task requires multiple such steps,
with sqlite3 either being run multiple times or
being made to engage in two-way communication with a driving process,
getting the combination to work as intended can be tedious and tricky.

Those difficulties are enough to make one wish for a better way
to drive the tool, and to muse: If only there was a "Tool Command Language".
Fortunately, there is; it is known as "Tcl".
The sqlite3 shell is a tool, in need of being flexibly commanded,
and Tcl is more than adequate for that purpose, as will be shown.

Not to be overlooked is that SQLite itself originated as a
library for Tcl, and that its integration with Tcl makes it both
powerful and easy to use in Tcl programs.
In fact, Tcl with SQLite can be a very good substitute for
ad hoc combinations of general purpose scripting tools with the SQLite shell,
and has been
[since year 2000](https://www.sqlite.org/src/timeline?c=2000-05-29+14:26:00).

Beyond the utility of [Tcl with SQLite](https://www.tcl.tk/about/uses.html),
SQLite (the shell) with Tcl has virtues of its own.
These include instrumentation,
query plan display, ready-to-use data import and export features,
among others familiar to sqlite3 shell users.

## Basics, Terminology and Getting Started ##

A variant of the SQLite shell can be built, called "sqlite3x" here,
whose long name is "The SQLite Extensible Shell".
It may be built on Unix-like systems by running "make sqlite3x".
(Build setup for Windows systems will be provided soon.)

The Tcl extension for sqlite3x, called "tclshext" here,
may be built for Unix-like systems,
after configure is run with a "--enable-tcl" option
(and a "--with-tcl=..." option if necessary),
by invoking make with "tcl_shell_extension" as a target.
It may be necessary to first install the Tcl development package or library
in order for configure to find the Tcl interpreter
and specify how get it linked into the extension.

To manually get a Tcl-extended-shell started,
(assuming the above-mentioned images were built
and have been placed where the shell and OS loader can find them),
either of these inputs is needed:<br>
```
  From a running shell:
    sqlite3x
    .shxload tclshext
 or
  At shell invocation:
    sqlite3x -cmd '.shxload tclshext'
```
(footnote: A directory path may need to be prepended to the extension's
name for the OS loader to find it unless it is in one of the locations
designated to the loader as a candidate for dynamic libraries. How such
designation is made is beyond the scope of this introduction.)
Provided this results in another prompt without any error message(s),
the Tcl-extended shell is ready to go.
For brevity, the shell in this state will be called "sqlite3xt".

## Yet Another Prompt -- Now What? ##

When sqlite3xt is ready, it acts very much like the sqlite3 shell.
When given sensible inputs (as discussed below in "Parsing"),
that are recognized by sqlite3,
the same outputs and effects will occur as would with sqlite3.
This condition,
where SQL and dot commands recognized by sqlite3 may be input
and acted upon as sqlite3 would,
is referred to below as the "shell execution environment".
For sqlite3xt, that is just the beginning.

## Execution Environments and Input Parsing Changes ##

The effect of loading tclshext can be briefly summarized as
the introduction of an alternative "execution environment",
explication of which follows this on the primary "execution environment":

### shell execution environment ###

The "shell execution environment", in effect upon sqlite3xt startup,
has these characteristics pertinent here:

* The leading non-whitespace token of an input line group
is interpreted as a dot command if it begins with a single '.';

* if that token is nothing but a '.', the line is a no-op;

* or, if it begins with anything but a '.', it is collected as SQL,
until terminated with ';', '/' or 'go',
then submitted to the SQL execution (prepare/step) engine.

* or, if it begins with '#', it is ignored (as a comment).

* Whitespace-delimited arguments after the leading token
are parsed according to the section on "Dot-command arguments"
[here](https://sqlite.org/cli.html), with one exception as noted next.

The exception to legacy argument parsing is that open quote
constructs are not auto-closed by the end of an input line.
(This legacy behavior may be reinstated if desired by: entering
".shxopts -parsing" at the prompt; renaming the sqlite3x image
to sqlite3 before executing it; or invoking it with the option
pair '-shxopt' '0'.)
Instead, arguments are collected, potentially across multiple lines,
until an input line ends outside of any open quote construct.
(Input which does not depend on the legacy, auto-close-on-newline
behavior is what the term "sensible inputs" means as used above.)
As examples, this would not be sensible input to sqlite3:<br>
```
   .print 'Hello.
   This was entered as multi-line input.'
```
as it would result in an error, while this input:<br>
```
   .print "I'm not fond of closing my quotations.
```
is acceptable to sqlite3, but deemed not "sensible" here.
When either is input to sqlite3x, a continuation prompt will be issued
(in interactive mode.)

Of course, this is (mostly) review to those familiar with the sqlite3 shell.

### Tcl execution environment ###

The "Tcl execution environment" differs in several important ways
from the familiar execution environment describe above.
How to get into this alternative execution environment is described later.

In this alternative execution environment, these critical differences exist:

* An expanded set of command words is available and readily expanded further.

* The available command words generally do not begin with '.'.

* When in interactive mode, commands whose
initial token is not defined as a Tcl command,
but can be found as an executable
in directories named in the PATH environment variable,
will be executed in a sub-process
(unless blocked by auto_noexec having been set.)

* The command word and arguments are collected, 
parsed and expanded according to the usual
[rules for the Tcl language](https://www.tcl.tk/about/language.html).
In particular, input line groups are collected until deemed "complete"
by the Tcl parser. This means no open brace, quote or bracket constructs.

* New [command words can be readily defined]
(https://www.tcl.tk/man/tcl8.6/TclCmd/proc.html),
and [variables can be set](https://www.tcl.tk/man/tcl8.6/TclCmd/set.html),
either of which may affect argument expansion
per the usual Tcl rules.

This environment will be quite familiar to those who use Tcl.
There are a few differences however. These are:<br>

* A single '.' on an input line which is not inside of an incomplete
Tcl line group is treated as a (momentary) end-of-input by the REPL.
(footnote: "REPL" is short for "Read, Evaluate, Print Loop.)

* The shell's dot commands, while not visible via \[info commands\],
are discoverable by the "unknown" command and will be executed
if their names (with the '.' prefix) would be found and resolved
unambiguously in the shell execution environment.
Commands whose names begin with '.' which are not found uniquely in the
shell execution environment produce an "invalid command name" error.
Except for that treatment, the unknown command in effect
acts like the standard Tcl version.
(footnote: That version remains available as _original_unkown,
to which non-dot commands are delegated .)

* A few non-standard-Tcl commands are available. In particular:

 + Commands udb and shdb act nearly like commands creatable
by the "sqlite3" Tcl package to represent an open database. They
differ in that they do not accept the "close" subcommand, which
reflects the fact that they exist to allow the shell's current
user database and the shell's own database to be accessed just
like ones created in Tcl via "sqlite3&nbsp;someDbName&nbsp;itsFilename".

 + Commands sqlite_shell_REPL, get_tcl_group and now_interactive
permit input to be collected in the same manner (and from the same
sources) as the shell's REPL does. Here, "collected in the same manner" 
does not include the execution environment switching or SQL execution
that the shell execution environment implements.

 + Command register_adhoc_command permits a newly (or oldly) defined
Tcl command, likely with a leading '.' in its name, to be associated
by name with some help text in a table kept by the shell
for augmentation of its .help facility.
In this way, the .help command can emit help
text, in summary or long forms, for Tcl commands that might be
executable from the shell execution environment.
(More on this below.)

 + Finally, the command ".." (sans quotes) exists for reasons made
evident below. With no arguments,
it does nothing, quietly and successfully, with the empty result.

* The present implementation does not run the event loop processing
that Tcl often uses for certain functionality (such as sockets.)

## Switching Execution Environments ##

The simplest execution environment switching
is effected with lone dots and dot pairs.
Some examples should make this clear. For now, focus on "." or ".."
entered on a line by itself.
(The # comments are not required. The leading prompts reflect
what would be seen in interactive use; they are not to be typed either.)<br>
```
   sqlite> # Now in shell execution environment, but it is time to do Tcl stuff.
   sqlite> ..
   tcl% # Now in Tcl execution environment, ready to roll a trivial dot command.
   tcl% proc .do {what} {
      >  .eval $what
      > }
   tcl% register_adhoc_command {.do   Does whatever one thing it is told to do
      >    from the shell execution environment and little else}
   tcl% # Time to return to shell execution environment.
   tcl% .
   sqlite> # See how ad hoc command creation worked.
   sqlite> .help do
   .do   Does whatever one thing it is told to do
      from the shell execution environment and little else
   sqlite> .do .conn
   ACTIVE 0: :memory:
   sqlite> .do ".. puts Putting"
   Putting
   sqlite> # Be sure about being in shell execution environment.
   sqlite> .
   sqlite> # Oh, the prompt would have sufficed. Now, for some Tcl ...
   sqlite> ..
   tcl% # Let's see what that .. command does.
   tcl% ..
   tcl% # Apparently, it either gets to the Tcl environment or stays there.
```

The use of lone . and .. to switch environments is easiest to understand.
However, as explained in the next section,
use of .. not alone on an input line exploits other Tcl functionality.

Another effect of loading tclshext is that a new dot command, .tcl ,
becomes part of the shell's repertoire.
When entered without any argument(s)
from the shell execution environment,
the .tcl command has the same effect as .. by itself.
(footnote:
When run from the Tcl execution environment with no arguments,
it acts as a no-op rather than entering a recursive REPL.)
With arguments, it will read file content into the Tcl interpreter
just as Tcl's source command would.
This may be useful for getting the Tcl execution environment
customized via sqlite3x invocation options such as
"'-cmd'&nbsp;'.tcl&nbsp;my_sqlite3xt_init.tcl'"

A side benefit of the .tcl command's existence is that
it appears in .help output, which can remind users how
to use the extension's main feature.

## Momentary Use of Tcl Execution Environment ##

When .. is not the sole non-whitespace content of an input line group
which has been submitted from the shell execution environment,
that causes argument collection and expansion to be performed
according to Tcl rules by the Tcl interpreter,
without entering and staying in the Tcl execution environment.
These two variations exist (where "..." stands for the provided argument set):
```
   sqlite> ..dotcmd ...
 or
   sqlite> .. tclcmd ...
```
The former, where no space separates .. from dotcmd, causes the
dot command known as .dotcmd to be executed (if it can be found.)
The latter, where space separates .. from tclcmd, causes the
Tcl command known as tclcmd to be executed (if it can be found.)
In either case, when the execution terminates (or fails with a
"command does not yet exist" or "invalid command name" error),
the shell execution environment remains in effect afterward.

This temporary use of the Tcl interpreter serves two purposes.
One is to exploit the more powerful capabilities of Tcl for argument processing.
Within the text of arguments as provided, 
variables can be accessed,
computations can be done,
and Tcl commands can be invoked to yield results (or produce side effects),
all of which can affect what the expanded arguments finally contain.
The other (miniscule) effect is to avoid the need for extra
input lines to switch to the Tcl execution environment and back
when "one-shot" use of Tcl is all that is needed.
In other words:<br>
```
   ..dotcmd whatever ...
 acts as a shorter form of
   ..
   .dotcmd whatever ...
   .
```

Whenever '..' leads an input line group
submitted in the shell execution environment,
then most of that input is given to the Tcl interpreter for
processing on a one-shot basis.

## Cross-Execution Environment Interactions ##

Study of the above examples might give rise to these questions:<br>
&nbsp;&nbsp;"How can '..dotcmd ...' work in the Tcl execution environment?"<br>
and<br>
&nbsp;&nbsp;"Why does '.do anything' work in the shell execution environment?"<br>
There are two features at work behind those examples working:

In the Tcl execution environment, (as touched upon above),
when a prospective Tcl command is not found to be defined
in the namespace(s) searched by the interpreter, it is
passed to the Tcl command named "unknown".
In the tclshext-augmented Tcl environment,
that procedure is implemented by C code which treats
a purported command with a leading '.' specially, 
by attempting to find it in the shell's repertoire of dot commands.
If found (unambiguously), it is then executed
and the result (such as it is) returned to the caller.
(footnote: The sqlite3 dot commands return the empty result on success.)
So, assuming there is a dot command invokable as .dotcmd,
(which there could be if another extension provided it),
it can be found and executed from the Tcl execution environment
with arguments as collected and expanded by the Tcl interpreter.

In the shell execution environment, if any extensions have been
loaded when a prospective dot command is not found in the
existing repertoire of dot commands kept by the shell,
the shell's dispatcher gives the (undocumented) .unknown
dot command a chance to execute it. This has no chance
of succeeding unless some extension has overridden the
built-in .unknown implementation. However, tclshext does
override it, and the replacement attempts to find any
dot command name it is given in the Tcl root namespace.
If it can be found, (such as will be true for the above
example invoking .do from the shell execution environment),
then that command is executed as a Tcl command
with whatever arguments were passed to .unknown .

Because of how this works, there is not often any reason or need to
leave the Tcl execution environment. Dot commands and Tcl commands
can be freely intermixed and executed in that environment. The main
reason one might revert to the shell execution environment would
be to evaluate SQL statements in the usual, sqlite3 way, (by just
typing them followed by ";".) However, SQL can be evaluated without
leaving the Tcl execution environment by use of the recently added
dot command, .eval, which evaluates its arguments in the shell
execution environment. For example:<br>
```
  tcl% .mode box
  tcl% .eval {
     >   .print "Let's have a one,"
     >   select 1 as one;
     >  } {
     >   .print "and a two."
     >   select 2 as two;
     >  }
  Let's have a one,
  ┌─────┐
  │ one │
  ├─────┤
  │ 1   │
  └─────┘
  and a two.
  ┌─────┐
  │ two │
  ├─────┤
  │ 2   │
  └─────┘
  tcl%
```

It should be noted, for those new to Tcl, that brace-quoting in Tcl
means "Whatever is between these matching braces is the (single) value."
It can be used for nearly any content, except for mismatched braces.

## Summary, More to Come ##

The Tcl extension should prove useful to those who like the features
of the sqlite3 shell but find using it programmatically to be challenging.

A future article will cover using Tk, a graphical user interface
toolkit originally created for use with Tcl, with sqlite3xt.
