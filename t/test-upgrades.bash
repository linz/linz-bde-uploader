#!/usr/bin/env bash

set -o errexit -o noclobber -o nounset -o pipefail
shopt -s failglob inherit_errexit

upgradeable_versions=(
    '2.5.2'
    '2.6.0'
    '2.7.0'
    '2.8.0'
    '2.8.1'
    '2.9.0'
    '2.10.0'
    '2.10.1'
)

project_root="$(dirname "$0")/.."
# Install all older versions
trap 'rm -r "$work_directory"' EXIT
work_directory="$(mktemp --directory)"
git clone "$project_root" "$work_directory"

test_database=linz-bde-uploader-test-db
export PGDATABASE="${test_database}"

for version in "${upgradeable_versions[@]}"
do
    dropdb --if-exists "${test_database}"
    createdb "${test_database}"

    psql -XtA <<EOF
CREATE SCHEMA IF NOT EXISTS _patches;
CREATE EXTENSION IF NOT EXISTS dbpatch SCHEMA _patches;
EOF

    echo "-------------------------------------"
    echo "Installing version $version"
    echo "-------------------------------------"
    git -C "$work_directory" clean -dx --force
    git -C "$work_directory" checkout "$version"
    "${work_directory}/configure" && make --directory="$work_directory"
    sudo env "PATH=$PATH" make --directory="$work_directory" install DESTDIR="$PWD/inst"

    # Install the just-installed linz-bde-uploader first !
    linz-bde-schema-load --revision "${test_database}"
    for file in inst/usr/local/share/linz-bde-uploader/sql/*.sql
    do
        echo "Loading $file from linz-bde-uploader ${version}"
        psql -o /dev/null -XtA -f "$file" "${test_database}" --set ON_ERROR_STOP=1
    done

# Turn DB to read-only mode, as it would be done
# by linz-bde-uploader-schema-load --readonly
    cat <<EOF | psql -Xat "${test_database}"
REVOKE UPDATE, INSERT, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA bde_control
    FROM bde_dba, bde_admin, bde_user;
EOF
    pg_prove -d "${test_database}" t/

done
