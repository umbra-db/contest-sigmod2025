#!/usr/bin/env bash

# check for files containing fmt
find engine/ -type f ! -path "*/all.cpp" ! -path "*/tools/*" ! -path "*/test/*" -exec grep "#include" {} \; | sort | uniq
