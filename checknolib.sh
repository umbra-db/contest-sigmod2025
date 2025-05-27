#!/usr/bin/env bash

# check for files containing fmt
find engine/ -type f ! -path "*/all.cpp" ! -path "*/tools/*" ! -path "*/test/*" -exec grep "fmt" {} \;

# check for files containing ranges
find engine/ -type f ! -path "*/all.cpp" ! -path "*/tools/*" ! -path "*/test/*" -exec grep "ranges" {} \;

# check for files containing Setting
find engine/ -type f ! -path "*/all.cpp" ! -path "*/tools/*" ! -path "*/test/*" -exec grep "Setting" {} \;

# check for files containing PerfEvent
find engine/ -type f ! -path "*/all.cpp" ! -path "*/tools/*" ! -path "*/test/*" -exec grep "PerfEvent" {} \;

# check for files containing Perfetto
find engine/ -type f ! -path "*/all.cpp" ! -path "*/tools/*" ! -path "*/test/*" -exec grep "Perfetto" {} \;
