#!/bin/bash

cd $(dirname $0)/..

find vumi/ -name '*.pyc' -delete

./manage.py test
trial --coverage --reporter=subunit vumi | subunit2junitxml > test_results.xml
