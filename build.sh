#! /bin/bash

FLAVOR=$1
if [ "$FLAVOR" == "/?" ]; then
  echo "usage:"
  echo "  $0 \<flavor\>"
  echo
  echo "where:"
  echo "   flavor  : Debug or Release"
  exit 0
fi

if [ "$FLAVOR" == "" ]; then
  echo "WARNING: missing flavor as parameter (see $0 /?)"
  echo "WARNING: using FLAVOR=Debug instead"
  echo
  FLAVOR=Debug
fi

xbuild /nologo /t:GenerateVersion /p:Configuration=$FLAVOR cassandra-sharp.targets
exit $?

