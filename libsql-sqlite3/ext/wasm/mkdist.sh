#!/usr/bin/env bash
###
#
# Builds the SQLite JS/WASM distribution zip file.
#
# Usage: $0 build-name ?flags?
#
# build-name is the dir/archive name prefix for the
# build and defaults to sqlite-wasm.
#
# -?|--help = Show this text
#
# -0    = Use -O0 instead of ${optFlag}
#
# -1    = Use make -j1 instead of ${makeFlag}
#
# -64   = Include 64-bit builds
#
# --noclean = do not run 'make clean' first
#
# --snapshot = gives the archive name a distinctive suffix
#
###

function die(){
    local rc=$1
    shift
    echo "Error: $@" 1>&2
    exit $rc
}

dirTop=../..
buildName=
b64=0
optFlag=-Oz
clean=1
makeFlag=-j4
snapshotSuffix=
for arg in $@; do
    case $arg in

        -64) b64=1
             ;;

        -0) optFlag=-O0
            ;;

        -1) makeFlag=
            ;;

        --noclean) clean=0
                   ;;

        --snapshot)
            snapshotSuffix=$(date +%Y%m%d)
            ;;

        -?|--help)
            sed -n -e '2,/^###/p' $0
            exit
            ;;

        *) if [[ x != x${buildName} ]]; then
               die 1 "Unhandled argument: $arg"
           fi
           buildName=$arg
           ;;
    esac
done

make=
for i in gmake make; do
    make=$(which $i 2>/dev/null)
    [[ x != x${make} ]] && break
done
[[ x = x$make ]] && die 127 "Cannot find make"


[[ x = x${buildName} ]] && buildName=sqlite-wasm

buildName=${buildName}${snapshotSuffix}

echo "Creating the SQLite wasm dist bundle..."

#
# Generates files which, when built, will also build all of the pieces
# neaded for the dist bundle.
#
tgtFiles=(
    demo-worker1-promiser.html
    demo-worker1-promiser.js
    demo-worker1-promiser-esm.html
    demo-worker1-promiser.mjs

    tester1.html
    tester1-esm.html
    tester1-worker.html
    tester1.js
    tester1.mjs
)

if [[ 1 = $b64 ]]; then
    tgtFiles+=(
        tester1-64bit.html
        tester1-esm-64bit.html
        tester1-worker-64bit.html
        tester1-64bit.js
        tester1-64bit.mjs
    )
fi

[[ 1 = $clean ]] && $make clean
$make $makeFlag \
      t-version-info t-stripccomments \
      ${tgtFiles[@]} \
      "emcc_opt=${optFlag}" || die $?

dirTmp=d.dist
rm -fr $dirTmp
mkdir -p $dirTmp/jswasm || die $?
mkdir -p $dirTmp/common || die $?

# Static files for the top-most dir:
fTop=(
    demo-123.html
    demo-123-worker.html
    demo-123.js

    demo-worker1.html
    demo-worker1.js

    demo-jsstorage.html
    demo-jsstorage.js

    module-symbols.html
)

# Files for the jswasm subdir sans jswasm prefix:
#
# fJ1 = JS files to stripccomments -k on
# fJ2 = JS files to stripccomments -k -k on
fJ1=(
    sqlite3-opfs-async-proxy.js
    sqlite3-worker1.js
    sqlite3-worker1.mjs
    sqlite3-worker1-bundler-friendly.mjs
    sqlite3-worker1-promiser.js
    sqlite3-worker1-promiser.mjs
    sqlite3-worker1-promiser-bundler-friendly.mjs
)
fJ2=(
    sqlite3.js
    sqlite3.mjs
)

# fW = list of wasm files to copy from jswasm/.
fW=(sqlite3.wasm)
if [[ 1 = $b64 ]]; then
    fW+=(sqlite3-64bit.wasm)
fi

function fcp() {
    cp -p $@ || die $?
    chmod +w ${@: -1}
}

function scc(){
    ${dirTop}/tool/stripccomments $@ || die $?
}

jw=jswasm
fcp ${tgtFiles[@]} $dirTmp/.
fcp README-dist.txt $dirTmp/README.txt
fcp index-dist.html $dirTmp/index.html
fcp common/*.css common/SqliteTestUtil.js $dirTmp/common/.

for i in ${fTop[@]}; do
    fcp $i $dirTmp/.
done

for i in ${fW[@]}; do
    fcp $jw/$i $dirTmp/$jw/.
done

for i in ${fJ1[@]}; do
    scc -k < $jw/$i > $dirTmp/$jw/$i || die $?
done

for i in ${fJ2[@]}; do
    scc -k -k < $jw/$i > $dirTmp/$jw/$i || die $?
done

#
# Done copying files. Now zip it up...
#
svi=./version-info
vnum=$($svi --download-version)
[ "" = "$vnum" ] && die "version number is empty!"
vdir=${buildName}-${vnum}
fzip=${vdir}.zip
rm -fr ${vdir} ${fzip}
mv $dirTmp $vdir || die $?
zip -rq9 $fzip $(find $vdir -type f | sort) || die $?
ls -la $fzip
unzip -lv $fzip || die $?
cat <<EOF
**
** Unzipped files are in $vdir
**
EOF
