# <u>Feature changes on shell-tweaks branch.</u>

This section summarizes the changes; motivation is addressed further below.

## (internal) Input stream management improvements:
* line number tracking centralized, done at arbitrary nesting
* source tracking centralized, done at arbitrary nesting
* input from either an open FILE or string content (for .x feature)
* stream switch save/restore simplified in several places


## Extension features made optional at build-time or runtime, for backwards compatibility. (Described further per-feature.)

## (internal) Query output display mode stack implemented and used.

(Associated ShellState reorg supports this, and groups members more by purpose.)

## (internal) Expand temp.sqlite_parameters usage to include "scripts".

An extra column, "uses", limits role of variables to either binding or execution. This is set when variables are created, keyed off their names.

## (internal) Refactoring and interface adjustments.

## Add --schema SCHEMA option to .dump

## Many changes and additions to .parameter subcommands:

### clear ?NAME?

Allowing selective zapping. (Was everything before.)

### edit ?OPT? NAME

Allow editing an existing or new entry. The OPT, if provided, may be -t or -e to select whether the resulting value is stored as text or evaluated as a SQL expression. A hidden option, --editor=\<something\>, may be used in place of having set environment variable DISPLAY before starting shell. This option is mentioned just when needed in an interactive session.

### list/ls output options

These are prettified with optional glob patterns.

### load ?FILE? ?NAMES?

Allow saved parameters to be loaded. With no arguments, a defaulted file in user's HOME directory is the source, and all of its parameters are loaded. With a FILE argument, named file is the source. With more arguments, selective loading from the file is done.

### save ?FILE? ?NAMES?

This is parallel to load except for direction of data flow and intolerance of FILE being read-only.

### set ?TOPT? NAME VALUE

Accept an optional type option to specify storage of the given value as a particular type, bypassing the old, "evaluate as SQL expression" behavior (which can be problematic without two levels of quoting.) Without the type option, the old behavior occurs. Also, if multiple arguments are found where VALUE would be, they are space-joined to form the stored value. (This is convenient for script use.)

### unset ?NAMES?

This is much like "clear ?NAMES?", except that with no names it is a no-op rather than an error as with prior shell versions.

## .shxopts command

This addition is to display or modify extension options.

## .x NAMES command

This will execute named parameters as if their content was fed to the input stream.

## Certain dot-commands become "undocumented".

This means that, in addition to not appearing in the website docs, they do not appear in the normal .help output. Help for them is available by entering ".help -all -all", and this is mentioned in the output of ".help -all" and ".help help".

Undocumented dot commands are: .check, .selftest-*, .seeargs, .testcase, and .testctrl .

## .seeargs command

An "undocumented" dot command to show arguments bounded by '|', for those who are unsure of the shell's quoting and expansion rules.

## For .open command

The --hexdb option without a FILE given can read in a hex DB from the current input stream, including from string sources.

## (internal) Compile option, BOOLNAMES_ARE_BOOLEAN

This adds "false" and "true" to the recognized binary switch values.

## (internal) Utility function, db_text()

This is added for reasons similar those for db_int().

## (internal) Preparation for variable expansion

This is for later addition of expansion within double-quoted command arguments.

## Dot-command parsing extension

This optional extension permits arguments to span more than one line, or more arguments to be added in subsequent lines. This is not the default behavior because it breaks backward compatibility (for ill-formed input with mis-balanced quotes.) It is enabled with either an invocation command-line option or the dot-commad, ".shxopts +parsing". With extended parsing, unclosed arguments may contain literal newlines, and lines may be be spliced if backslash is the final character (other than newline) at the end of an input line.

## (internal) Rewrite of process_input()

It was rewritten to support collection and dispatch of multi-line input in a more regular manner. It is now over-commented to aid initial write and review if desired. Its comment volume is slated for post-merge reduction.

## (semi-internal) An undocumented command-line option, -quiet

This was added to suppress the start-up banner (which contains a version string) and prompts when input is "interactive".

## (internal) Tests added for above new/changed features.

## (internal) mkshellc.tcl usefully handles simple typedefs with comments.

# <u>Motivation or justification for above shell-tweaks branch changes.</u>

The changes marked "(internal)" are generally made to either simplify the code, or to manage its complexity in a more readily understood way. It can mostly be categorized as "showing a little love" for the affected code where not strictly necessary for feature implementation.

## Input stream management improvements

This set of changes makes it easier to manage input streams, as can be seen by the impact on client code where that happens. Additionally, better error reporting is made possible from within nested sources.

## Extension features made optional

The build-time feature trimming option is for resource-constrained environments. It also serves the purpose of showing where code can be chopped out or how it can be adapted if the feature is vetoed. It may also show the feature's impact on code volume.

Runtime feature enable/disable is for features that impact backward compatibility. For example, with present releases, if the last argument for a dot command has an opening quote but no closing quote, it is considered closed by the end-of-line. With the command parsing extension enabled, such a construct begins an argument spanning more than one line. To avoid breaking old script input to the CLI, which may have such ill-formed arguments, the extension is disabled by default.

## Query output display mode stack

This is mainly a clean-up of existing functionality. It also anticipates future mode save/restore that may be exposed to users.

## Expand temp.sqlite_parameters usage to include "scripts"

This internal change reflects an implementation choice in support of scripting. It could have been done with a separate table, but this simplifies the code, UI, and documentation (to come.)

## Refactoring and interface adjustments to allow resuse.

Clean-up and maintenance burden reduction mainly motivates this.

## Add --schema SCHEMA option to .dump

Simplifies scenarios where a user attaches a DB, or creates an attached DB, for the purpose of transfering data to it from a main DB and then saving it out in .dump format. Similar convenience is made available for the TEMP DB.

## Changes and additions to .parameter subcommands

The .parameter command becomes the focal point for managing all kinds of user-alterable parameters and variables. It is easy to remember: If some parameter(s) will be created, modified, reviewed, saved, loaded, or removed, .parameter is how it is done.

### clear or unset ?NAMES?

Make it convenient to remove just selected parameters, (rather than all of them as before), for pre-save cleanup, list de-clutter, or testing effect of unbound query parameters.

### edit ?OPT? NAME

Allow convenient interactive creation or modification of parameter values, particularly ones that may have multiple lines or tax a user's perception when only a line-editor is available. The feature is only enabled for interactive sessions, as it makes little sense to begin an edit session in batch mode. Its hidden option, --editor=\<something\>, is for cases where a user has not set DISPLAY before starting the CLI, or needs to change it. It does not appear in help, but is mentioned when edit is invoked without DISPLAY being set.

### load/save ?FILE? ?NAMES?

This persistence facility is mainly for edited queries, DDL or DML that a user may wish to reuse across sessions. Or it may prove useful for commonly repeated sequences of dot commands. Or both.

### list/ls output options

These make it easier to see what is in the parameters table by using the short, "ls" form, which lists only names, or by specifying glob patterns to restrict output. With the added utility of parameters, there may be a lot more to see or avoid seeing.

### set ?TOPT? NAME VALUE

The new option, TOPT, can force the value's type or to bypass the effective eval trial for text. This makes it more deterministic for those who may not understand the detailed processing or quoting rules. Taking the VALUE to be the space-joined remaining arguments simplifies quoting in many cases.

## Added .shxopts command

By allow certain optional features, which potentially impair backward compatibility, to be enabled or disabled at runtime, users are given control over the trade between compatibility and the convenience of the new features.

## The new dot command, .x NAMES

This is a convenience for those who are often repeating some command, query, or set of commands and/or queries. Many people do not get their queries right on the first write.

## Certain dot-commands become "undocumented"

This is to reduce clutter seen by those who use the CLI to use SQLite rather than to test it. It may also reduce needless doc searches by the curious.

## Compile option, BOOLNAMES_ARE_BOOLEAN

This is a place-holder for until addition of "false" and "true" to the binary switch name set is rejected or accepted. The case for adding them is that they are a natural choice as boolean values and having "true" taken as "false" (as happens today) can be surprising.

## Extended dot-command parsing

This feature, (where arguments and/or dot commands may span line-ends), has several uses. One is for allowing ".param set \<name\> \<value\>" to specify a multi-line value, useful mainly for script creation. (Otherwise, multi-line script, which may contain complex SQL in need of line-structure, can only be created by using ".parameter edit \<name\>".) Another is for .print commands, so that literal multi-line output may be more naturally specified.

## process_input() rewrite

This was needed to support extended dot-command parsing. The difficulty of gaining that support without a rewrite, together with anticipation of later adding a TCL input feature, (which would present similar issues), easily made the rewrite appear advantageous. The code was hard to follow before, but can now be easily adapted to incorporate TCL scripting when (or if) that is added.

## added command-line option, -quiet

This facilitates testing of the ".parameter edit" feature.

## mkshellc.tcl change to handle more typedef repetition than before

This merely reduces the fragility of the feature. It was motivated by added use of a sometimes-redundant typedef which exposed the older fragility.
