#!/bin/bash

set -e

echo "===== Testing sqlite-bfsvtab-ext ======"

ls test/test_*.sh | xargs -I - bash -

echo "========= Testing Completed ==========="

