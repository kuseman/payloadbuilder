Changelog

## [v0.6.0](https://gitservice/v0.6.0) (2022-04-07)


### Bug Fixes

 -  **core**  Add support for system catalog. Catalogs can implement system operators for showing columns, tables etc. ([82357d02ed0b1a3](https://gitservice/commit/82357d02ed0b1a36ea4d6071ff298cc4cebd09e3))

## [v0.5.0](https://gitservice/v0.5.0) (2022-03-18)


### Bug Fixes

 -  Add support for describing a table, showing indices etc. ([6e54d8874fd117c](https://gitservice/commit/6e54d8874fd117c82b0bc299f8f92ab7817bbafa))
 -  Check unresolved expressions in fold method ([362605e3e8b7fa9](https://gitservice/commit/362605e3e8b7fa95cb8cfa19c44fd25637b68930))
 -  **core**  Bumped dependencies ([8dca4e9810f93bb](https://gitservice/commit/8dca4e9810f93bb7ce327507812a3b0006877a2d))
 -  **core**  Fixed issue with apply with subquery without table source ([44c7ba82c536f86](https://gitservice/commit/44c7ba82c536f864f8e6735314976d42aba659af))
 -  **core**  Refactor of parsing/resolving into 2:phases. ([58e95fb71a26cd5](https://gitservice/commit/58e95fb71a26cd5468878a9630b5b640c148c93b))
 -  **core**  Remove subTuple concept and instead have unique tuple ordinals for total query ([f7f588441c3a327](https://gitservice/commit/f7f588441c3a327577e128c206cc85399293a3e1))
 -  **core**  Fixed some minor resolving issues with temp tables ([16a86bd54f0cc3f](https://gitservice/commit/16a86bd54f0cc3f91ce3dd68fd07a96633a627f9))
 -  **core**  Rename indexValues to ordinalValues ([1b7f9cb788dcebd](https://gitservice/commit/1b7f9cb788dcebd1760bc1d9c5e716618cc180a9))
 -  **core**  Add support for writing csv with asterisk select ([1d49efed250515c](https://gitservice/commit/1d49efed250515c7ba93a1446541dda869d17137))

## [v0.4.0](https://gitservice/v0.4.0) (2021-12-01)


### Bug Fixes

 -  **core**  Add settings to Json and Csv writers ([a375bf436b32709](https://gitservice/commit/a375bf436b327096424375a952fe5336823d7307))

## [v0.3.0](https://gitservice/v0.3.0) (2021-11-29)



## [v0.0.3](https://gitservice/v0.0.3) (2021-11-29)

### Features

 -  **core**  Add support for sub query expressions ([589cda2527b8711](https://gitservice/commit/589cda2527b871158c0c7ee55d5b140685d8b9ad))
 -  **core**  Add support for real sub queries with select items, computed expressions ([04ae0617271d83d](https://gitservice/commit/04ae0617271d83d394cf11a770bd4c4a082422c4))
 -  **core**  Added support for generating java code of predicates with Janino to increase performance ([e4b5ff587b42b57](https://gitservice/commit/e4b5ff587b42b57fdfedcc3796c952a6f8ab1516))
 -  **core**  Add support for analyzing select statements and leting catalogs add information ([c1427fd3b0ddd7b](https://gitservice/commit/c1427fd3b0ddd7be44b557c75c126da2a63e44f2))
 -  **core**  Temp table support via INSERT INTO ([665d9f7a48b9cf6](https://gitservice/commit/665d9f7a48b9cf676be6f0b5249c9eb81289f195))
 -  **core**  Add support for system variable @@rowcount ([5296cfa63e0552a](https://gitservice/commit/5296cfa63e0552a946d4db12711c57afccc39e4f))

### Bug Fixes

 -  Bump dependencies ([30b4f4c95d579f1](https://gitservice/commit/30b4f4c95d579f15ce4abeae1667762f7abd9db4))
 -  Checkstyle ([976e27a5226dff2](https://gitservice/commit/976e27a5226dff245c0b8a97202ef09639e8c804))
 -  Checkstyle ([0321c7740064889](https://gitservice/commit/0321c774006488936f743c713aa8d233d0d16557))
 -  Checkstyle ([1b4a8018df1e1cf](https://gitservice/commit/1b4a8018df1e1cfac92d698c77664ca92dfa4a2a))
 -  **core**  Change table alias structure to properly build a new hierarchy per sub query ([ccf946f7df4eda7](https://gitservice/commit/ccf946f7df4eda72ea2b073f65ae56d5933b2b75))
 -  Bug where a Tuple was wanted but ok with non tuple like Map ([2c9d9b4d1b715c4](https://gitservice/commit/2c9d9b4d1b715c466b6acb3e187b82a2f75dbda2))
 -  **core**  MAJOR rewrite of tuple structure and how data is fetched to a more performant version ([3f75c3ed8457cab](https://gitservice/commit/3f75c3ed8457cabbdf3208563287dff1994f5b1e))
 -  Separte system variables from program ([454c1b52ec44d49](https://gitservice/commit/454c1b52ec44d49cfa142e48e8fc265465cf7a12))

