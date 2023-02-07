#!/usr/bin/env bash
ARCHIVE="packages.zip"

if [ -f "./$ARCHIVE" ]
then
    rm "./$ARCHIVE"
fi

pip install -r requirements.txt --target ./packages

# if there are no packages - create an empty file so zip doesn't fail
if [ -z "$(ls -A packages)" ]
then
    touch ./packages/empty.txt
fi

if [ ! -d packages ]
then
    echo "pip failed to download requirements"
    exit 1
fi

pushd ./packages

zip -9mr "../$ARCHIVE" .

popd

rm -rf ./packages

zip -9ur "$ARCHIVE" common -x common/__pycache__/\*