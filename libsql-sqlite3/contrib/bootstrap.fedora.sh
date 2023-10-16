#!/bin/sh
# Originated from https://git.io/autobuild 
#
# This script performs rpmbuild environment setup and the initial autotools bootstrapping.
# Abort the script and exit with failure if any command below exits with
# a non-zero exit status.
set -e


# Check needed software for building libsql on fedora
[ -e /etc/redhat-release ] || (echo "Not on fedora based System";exit 1)
OS=`cat /etc/redhat-release`
[ -e /usr/bin/gcc      ]   || (echo "sudo dnf install -y gcc ";exit 1)
[ -e /usr/bin/make     ]   || (echo "sudo dnf install -y make ";exit 1)
[ -e /usr/bin/libtool  ]   || (echo "sudo dnf install -y libtool ";exit 1)
[ -e /usr/bin/autoconf ]   || (echo "sudo dnf install -y autoconf";exit 1)
[ -e /usr/bin/automake ]   || (echo "sudo dnf install -y automake";exit 1)
[ -e /usr/lib64/tclConfig.sh ] || (echo "sudo dnf install -y tcl-devel";exit 1)
[ -e /usr/include/readline/chardefs.h ] || (echo "sudo dnf install -y readline-devel";exit 1)
[ -e /usr/include/unctrl.h ] || (echo "sudo dnf install -y ncurses-devel";exit 1)
echo "compiling environment on ${OS} is OK now."

# Create the m4/ directory if it doesn't exist.
[ -d m4 ] || mkdir m4
 
# If there's configure script, reconfigure the autoconf files. Make sure
# to install missing files and re-run configure and make if needed.
#[ -e ./configure ] || autoreconf -im
#aclocal && automake --gnu --add-missing && autoconf
#[ -e ./configure ] ||  ( rm -f configure && aclocal && autoconf )
[ -e ./configure ] || (aclocal && autoconf )

# If the Makefile doesn't exist, the previous step didn't run; this
# indicates the presence of a configure script. Run that script and
# then call make.
[ -e ./Makefile  ] ||  (rm -f Makefile)
./configure 
 
# If src/codename doesn't exist, there was a Makefile but make hasn't
# been run yet. Run it, which should produce the codename binary.
# Last step of make is cp sqlite3 to libsql
[ -e ./libsql  ] || make
