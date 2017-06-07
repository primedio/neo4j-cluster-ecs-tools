#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    # feedback
    echo "Processing signing key..."
    # decrypt signing key
    openssl aes-256-cbc -K $encrypted_c93bc20805ed_key -iv $encrypted_c93bc20805ed_iv -in .travis/codesigning.asc.enc -out .travis/codesigning.asc -d
    # import signing key
    gpg --fast-import .travis/codesigning.asc
fi

exit 0
