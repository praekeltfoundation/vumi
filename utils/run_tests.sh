#!/bin/bash -e

cd $(dirname $0)/..

find vumi/ -name '*.pyc' -delete

coverage erase
trial --reporter=subunit vumi | subunit2junitxml > test_results.xml
trial --reporter=bwverbose-coverage vumi
./manage.py test --with-xunit --xunit-file=django_test_results.xml --cover-package=vumi.webapp
coverage xml
pep8 --repeat --exclude '0*.py' vumi > pep8.txt
