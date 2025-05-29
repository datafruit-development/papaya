#! /bin/bash

# ingests the entire relevant codebase to your clipboard
gitingest . -o papaya_digest.txt \
  -e "target/,__pycache__/,*.cache,*.zip,*.lock,.ropeproject/,project/target/,docs/,*.pyc,streams/,sync/,update/,classes/,zinc/,*.properties,plugin/project/,plugin/target/,.python-version,global-logging/,task-temp-directory/,*_cache*,*.class,inc_compile*" && cat papaya_digest.txt | pbcopy
rm papaya_digest.txt
