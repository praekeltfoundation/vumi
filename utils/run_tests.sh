#!/bin/bash

cd $(dirname $0)/..

find vumi/ -name '*.pyc' -delete

./manage.py test --with-xunit --xunit-file=django_test_results.xml
trial --reporter=subunit vumi | subunit2junitxml > test_results.xml
trial --reporter=bwverbose-coverage vumi && coverage xml
pep8 --repeat --exclude '0*.py' vumi > pep8.txt
