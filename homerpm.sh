#!/bin/sh

echo "%HOME %{expand:%%(cd; pwd)}" > $HOME/.rpmmacros
echo "%_topdir %{HOME}/rpm" >> $HOME/.rpmmacros

mkdir $HOME/rpm
mkdir $HOME/rpm/BUILD
mkdir $HOME/rpm/SOURCES
mkdir $HOME/rpm/RPMS
mkdir $HOME/rpm/SRPMS
mkdir $HOME/rpm/SPECS
