Contribution guidelines
=======================

If you want to contribute to MPIRE, great! Please follow the steps below to ensure a smooth process:

1. Clone the project.
2. Create a new branch for your feature or bug fix. Give you branch a meaningful name.
3. Make your feature addition or bug fix.
4. Add tests for it and test it yourself. Make sure it both works for Unix and Windows based systems, or make sure to
   document why it doesn't work for one of the platforms.
5. Add documentation for it. Don't forget about the changelog:

   - Reference the issue number from GitHub in the changelog, if applicable (see current changelog for examples).
   - Don't mention a date or a version number here, but use ``Unreleased`` instead.

6. Commit with a meaningful commit message (e.g. the changelog).
7. Open a pull request.
8. Resolve any issues or comments by the reviewer.
9. Merge PR by squashing all your individual commits.

Making a release
----------------

A release is only made by the project maintainer. The following steps are required:

1. Update the changelog with the release date and version number. Version numbers follow the `Semantic Versioning`_
   guidelines
2. Update the version number in ``setup.py`` and ``docs/conf.py``.
3. Commit and push the changes.
4. Make sure the tests pass on GitHub Actions.
5. Create a tag for the release by using ``git tag -a vX.Y.Z -m "vX.Y.Z"``.
6. Push the tag to GitHub by using ``git push origin vX.Y.Z``.

.. _Semantic Versioning: https://semver.org/
