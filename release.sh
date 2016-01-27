#!/usr/bin/env bash
git checkout master
git pull
git checkout PRODUCTION
git merge master --no-edit
git push origin PRODUCTION
