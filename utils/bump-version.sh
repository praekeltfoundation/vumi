#!/bin/bash
VER="$1"

if [[ "x${VER}" = "x" ]]
then
    echo "Usage: $0 <version number>"
    echo " e.g. $0 0.1.0"
    exit 1
fi

sed -i".bak" -e "s/\(version[ ]*=[ ]*[\"']\)\(.*\)\([\"'].*\)/\1${VER}\3/" setup.py