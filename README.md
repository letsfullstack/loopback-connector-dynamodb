![Publish Github Package](https://github.com/letsfullstack/loopback-connector-dynamodb/workflows/Publish%20Github%20Package/badge.svg)

# Loopback DynamoDB Connector

DynamoDB Connector for loopback (compatible with datasource-juggler).

## Contributing

:boom: In case you are making a commit for this package repository, **MAKE SURE TO READ AND UNDERSTAND THE FOLLOWING TOPICS**:

1\. Every commit that runs on the [master branch](https://github.com/letsfullstack/loopback-connector-dynamodb/tree/master) runs through the Publish Github Package Workflow on Github Actions. So **be sure to check if your code is well written and linted**, since it'll be published if the code passes the Continuous Integration (CI) linters. Testing wasn't properly performed in the original repository and still lacks proper corrections in this repo.

2\. If the commit passes through the Github Actions workflow, the module will be released as a package in the Github Packages Registry. This workflow has an [underlying command](https://github.com/phips28/gh-action-bump-version) that **increments/bumps the version from the latest release based on commit messages**, such as:

- If the string "BREAKING CHANGE" or "major" is found anywhere in any of the commit messages or descriptions, the **major version** will be incremented (i.e. 1.X.X).

- If a commit message begins with the string "feat" or includes "minor" then the **minor version** will be increased (i.e. X.1.X). This works for most common commit metadata for feature additions: "feat: new API" and "feature: new API".

- All other changes will increment the **patch version** (i.e. X.X.1).

3\. Furthermore, the workflow has also an underlying command that deploys automatically a new release when a success lint/deployment takes places. These releases can be found [here](https://github.com/letsfullstack/loopback-connector-dynamodb/releases).

## Installation

In your application root directory, enter this command to install the connector:

```shell
$ npm install @letsfullstack/loopback-connector-dynamodb
```

## Todo

- Usage description (how to usage)

- Fix unit tests