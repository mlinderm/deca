Notes for release managers
---

This document describes how to make an Deca release.

Setup your environment
1. Copy (or incorporate) the settings.xml file to ```~/.m2/settings.xml```
2. Request the Deca packager private GPG key
3. Edit the username, password, etc in ```~/.m2/settings.xml```

Once your environment is setup, you'll be able to do a release.

First update CHANGES.md with closed issues and closed and merged pull requests and commit to master.
Since version 0.18.2 this is a manual process, see issue #936 and e.g. commit 48e2930a4bb026928b31e5bef03b305f0dbeb41d.

Then from the project root directory, run `./scripts/release/release.sh`.
If you have any problems, run `./scripts/release/rollback.sh`.

Once you've successfully published the release, you will need to "close" and "release" it following the instructions at
http://central.sonatype.org/pages/releasing-the-deployment.html#close-and-drop-or-release-your-staging-repository

After the release is rsynced to the Maven Central repository, confirm checksums match and verify signatures.

Create a new pull request against [Homebrew Science](https://github.com/Homebrew/homebrew-science) for the new release version.
Often this will just require a version bump and checksum update.  See e.g. https://github.com/Homebrew/homebrew-science/pull/3375.

Finally, be sure to announce the release on the ADAM mailing list and Twitter (@bigdatagenomics).