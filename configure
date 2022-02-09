#!/usr/bin/env bash

set -o errexit -o noclobber -o nounset -o pipefail
shopt -s failglob inherit_errexit

PERL=perl

cd `dirname $0`

${PERL} Build.PL
test $? = 0 || exit 1

./Build distmeta # builds Makefile.PL (among other things)
test $? = 0 || exit 1

./Build manifest # builds MANIFEST
test $? = 0 || exit 1

${PERL} Makefile.PL
test $? = 0 || exit 1

# Append custom rules
cat <<EOF >> Makefile
deb:
	dpkg-buildpackage -b -us -uc

check:
	\$(MAKE) test TEST_VERBOSE=1

check-upgrades:
	t/test-upgrades.bash
EOF
