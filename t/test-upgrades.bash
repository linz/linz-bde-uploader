#!/usr/bin/env bash

set -o errexit -o noclobber -o nounset -o pipefail
shopt -s failglob inherit_errexit

upgradeable_versions="
    2.5.2
    2.6.0
"

test_database=linz-bde-uploader-test-db

git fetch --tags # to get all commits/tags

tmpdir=/tmp/linz-bde-uploader-test-$$
mkdir -p "${tmpdir}"

export PGDATABASE="${test_database}"

for ver in ${upgradeable_versions}; do
    owd="$PWD"

    dropdb --if-exists "${test_database}"
    createdb "${test_database}"

    psql -XtA <<EOF
CREATE SCHEMA IF NOT EXISTS _patches;
CREATE EXTENSION IF NOT EXISTS dbpatch SCHEMA _patches;
EOF

    cd "${tmpdir}"
    test -d linz-bde-uploader || {
        git clone --quiet --reference "$owd" \
            https://github.com/linz/linz-bde-uploader
    }
    cd linz-bde-uploader
    git checkout "${ver}"
    ./configure && make
    sudo env "PATH=$PATH" make install DESTDIR="$PWD/inst"

    # Install the just-installed linz-bde-uploader first !
    linz-bde-schema-load --revision "${test_database}"
    for file in inst/usr/local/share/linz-bde-uploader/sql/*.sql
    do
        echo "Loading $file from linz-bde-uploader ${ver}"
        psql -o /dev/null -XtA -f "$file" "${test_database}" --set ON_ERROR_STOP=1
    done

    cd "${owd}"

# Turn DB to read-only mode, as it would be done
# by linz-bde-uploader-schema-load --readonly
    cat <<EOF | psql -Xat "${test_database}"
REVOKE UPDATE, INSERT, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA bde_control
    FROM bde_dba, bde_admin, bde_user;
EOF
    pg_prove -d "${test_database}" t/

done
