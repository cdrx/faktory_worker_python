# [Changelog](https://github.com/cdrx/faktory_worker_python/releases)

All notable changes to this project will be documented in this file.

## 1.0.0 - 2022-04-30

- Python 3.7+ is now required (#36)
- Fixed a type issue that crashed the worker (#41)
- Improved recovery from broken process pool error, thanks to @tswayne (#42)

## 0.5.1 - 2021-03-20

- Fixed issue with installing 0.5.0

## 0.5.0 - 2021-03-01

- This release was yanked due to a build issue -- use 0.5.1 instead
- Added support for the `backtrace` option, thanks to @tswayne

## 0.4.0 - 2018-02-21

- Added support for job priorities
- Added validation if args isn't something faktory will accept
- Added a pool of threads based worker implementation
- Compatibility with Faktory 0.7
