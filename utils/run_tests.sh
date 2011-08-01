#!/bin/bash

cd $(dirname $0)/..

find vumi/ -name '*.pyc' -delete

./manage.py test #-- --with-xunit --xunit-file=django_test_results.xml
trial --coverage --reporter=subunit vumi | subunit2junitxml | grep --line-buffered -Fv 'Setting coverage directory to coverage.' | tee test_results.xml
