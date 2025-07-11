#!/bin/bash
set -ex
echo "I am alive"
ls -l /app/bin/run-glicko
/app/bin/run-glicko --help
echo "Done"
