#!/usr/bin/env bash

set -o errexit -o noclobber -o nounset -o pipefail
shopt -s failglob inherit_errexit

UPGRADEABLE_VERSIONS="
    2.5.2
    2.6.0
"

TEST_DATABASE=linz-bde-uploader-test-db

git fetch --tags # to get all commits/tags

TMPDIR=/tmp/linz-bde-uploader-test-$$
mkdir -p "${TMPDIR}"

export PGDATABASE="${TEST_DATABASE}"

for ver in ${UPGRADEABLE_VERSIONS}; do
    OWD="$PWD"

    dropdb --if-exists "${TEST_DATABASE}"
    createdb "${TEST_DATABASE}"

    psql -XtA <<EOF
CREATE SCHEMA IF NOT EXISTS _patches;
CREATE EXTENSION IF NOT EXISTS dbpatch SCHEMA _patches;
EOF

    cd "${TMPDIR}"
    test -d linz-bde-uploader || {
        git clone --quiet --reference "$OWD" \
            https://github.com/linz/linz-bde-uploader
    }
    cd linz-bde-uploader
    git checkout "${ver}"
    ./configure && make
    sudo env "PATH=$PATH" make install DESTDIR="$PWD/inst"

    # Install the just-installed linz-bde-uploader first !
    linz-bde-schema-load --revision "${TEST_DATABASE}"
    for file in inst/usr/local/share/linz-bde-uploader/sql/*.sql
    do
        echo "Loading $file from linz-bde-uploader ${ver}"
        psql -o /dev/null -XtA -f "$file" "${TEST_DATABASE}" --set ON_ERROR_STOP=1
    done

    cd "${OWD}"

# Turn DB to read-only mode, as it would be done
# by linz-bde-uploader-schema-load --readonly
    cat <<EOF | psql -Xat "${TEST_DATABASE}"
REVOKE UPDATE, INSERT, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA bde_control
    FROM bde_dba, bde_admin, bde_user;
EOF
    pg_prove -d "${TEST_DATABASE}" t/

done
