#!/bin/bash

cd $(dirname $0)/..

echo "=== Nuking old .pyc files..."
find vumi/ -name '*.pyc' -delete
echo "=== Erasing previous coverage data..."
coverage erase
echo "=== Running trial tests..."
coverage run `which trial` --reporter=subunit vumi | tee results.txt | subunit2junitxml > test_results.xml
subunit2pyunit < results.txt
rm results.txt
echo "=== Processing coverage data..."
coverage xml
echo "=== Checking for PEP-8 violations..."
pep8 --repeat --exclude '0*.py' vumi > pep8.txt
echo "=== Done."
