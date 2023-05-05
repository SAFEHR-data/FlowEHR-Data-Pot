# Example Data Pipeline on FlowEHR

This repository provides an example of how to create a Data Pipeline that can be deployed as part of [FlowEHR](https://github.com/UCLH-Foundry/FlowEHR).

Using this example, you can build and deploy data transformation code and test it locally via unit tests. Then, you can test it within your dev environment in Azure. Finally, it can be integrated with your deployment of FlowEHR.

## Quick start 

Step 1: Clone this repository

Step 2: _From your cloned repository_, press the "Open in Remote - Containers" below

[
    ![Open in Remote - Containers](
        https://img.shields.io/static/v1?label=Remote%20-%20Containers&message=Open&color=blue&logo=visualstudiocode
    )
](
    https://vscode.dev/redirect?url=vscode://ms-vscode-remote.remote-containers/cloneInVolume?url=https://github.com/UCLH-Foundry/FlowEHR-Data-Pot.git
)

## Prerequisites

The following guide assumes you know how to create a Data Pipeline on Databricks and PySpark. It also assumes general familiarity with Azure, Devcontainers, Makefile, Azure Data Factory, and Terraform.

## Local development

For local development, there is a Devcontainer configuration of this repository.
To start using it, [start your Devcontainer environment](https://code.visualstudio.com/docs/devcontainers/tutorial). 

### Running tests locally
To run tests, navigate into `hello-world` directory in the Terminal:

```
cd hello-world
```

Now you can run unit tests for the Data Pipeline. In the Terminal, run

```Makefile
make test
```

The source code for tests can be found in [./hello-world/tests/](./hello-world/tests/).

You are strongly urged to write individual unit tests for each transform in the data pipeline.

## Integration with FlowEHR

To make sure the Data Pipeline can be deployed by FlowEHR, the pipeline must have two components:

1. An `pipeline.json` file. This should conform to the way a pipeline is defined in Azure Data Factory. It is not created independently, rather, it is dependent on FlowEHR in the implementation details such as names of linked services, paths and so on.

See [example](./hello-world/pipeline.json) in the repo:

```json
{
  "name": "PatientsPipeline",
  "properties": {
      "activities": [
          {
              "name": "DatabricksPythonActivity",
              "type": "DatabricksSparkPython",
              "typeProperties": {
                  "pythonFile": "dbfs:/pipelines/hello-world/artifacts/entrypoint.py",
                  "libraries": [
                      {
                          // Will be uploaded to DBFS by a script in FlowEHR repo
                          // More libraries can be added here, e.g. from PyPi
                          "whl": "dbfs:/pipelines/hello-world/artifacts/hello_world-0.0.1-py3-none-any.whl"
                      }
                  ]
              },
              "linkedServiceName": {
                  "referenceName": "ADBLinkedServiceViaMSI",  // Needs to be in sync with what is set in FlowEHR
                  "type": "LinkedServiceReference"
              }
          }
      ],
      "parameters": {}
  },
  "type": "Microsoft.DataFactory/factories/pipelines"
}  
```

In there: 
- `linkedServiceName.referenceName` must be set to the same value as it is [set on FlowEHR repo](https://github.com/UCLH-Foundry/FlowEHR/blob/main/infrastructure/transform/locals.tf#L19).
- `pythonFile` and `libraries` must have a format like this:
`dbfs:/pipelines/{pipeline_name}/artifacts/{artifact_name}`. 

The rest can be copied over from the example above.

2. A `Makefile` at the root of the repository with an `artifacts` target that will build any artifacts that the pipeline needs to run and put all files that need uploading into `artifacts` directory at the root of the pipeline.

See [example](./hello-world/Makefile) in the repo:

```Makefile
artifacts: build
	mkdir -p artifacts
	cp entrypoint.py artifacts
	cp dist/*.whl artifacts/
```

For a PySpark pipeline, it is recommended to put any library or shared code into a .whl file, as this way it is more straightforward to write automated tests for. There is an [entrypoint.py](./hello-world/entrypoint.py) that is required by Azure Data Factory to be present that is just executing the code that is present in the library.

Note that the name of the artifact must match exactly the name specified in `pipeline.json`.

## Deploy the pipeline code into a dev environment 

To deploy the pipeline in the context of FlowEHR, please follow these steps:

1. Clone [FlowEHR repository](https://github.com/UCLH-Foundry/FlowEHR).

1. Follow the [steps outlined in the README](https://github.com/UCLH-Foundry/FlowEHR#getting-started) to set up your dev environment and deploy FlowEHR. As part of this, you will need to run `make infrastructure-transform`. 

1. Use one of the following options to reference your pipeline code in FlowEHR repo:

    * Copy over your pipeline code (together with a Makefile and an `pipeline.json`) into `/transform/pipelines` into your checked-out copy of FlowEHR. (Note that `/transform/pipelines` is gitignored in FlowEHR). You should have a directory tree similar to the following:

    ```
    └── transform
        └── pipelines
            ├── my-pipeline 
            │   ├── Makefile
            │   ├── pipeline.json
            │   ├── src
            │   │   └── ...
            │   ├── tests
            │   │   └── ...
            │   └── ...
    ```

    * In `config.transform.yaml` at the root of the FlowEHR repository, provide a link to your Data Pipeline repository. Then, rebuild your Devcontainer (repositories are checked out on Devcontainer startup).
    ```
    ---
    spark_version: 3.3.1
    repositories:
     - git@github.com:tanya-borisova/my-pipeline.git
    ```

1. Re-run `make infrastructure-transform` from the root of FlowEHR while running in your Devcontainer. In the log output, you should see new resources of type `azurerm_data_factory_pipeline.pipeline` being created. 
If at this stage you don't see the resources being created, double-check that you have both `Makefile` and `pipeline.json` at the root of your pipeline directory in `transform/pipelines`.

1. In Azure Portal, navigate to the instance of Azure Data Factory deployed in your environment. Launch Studio, then on the left under Author find Pipelines. If everything is correct, you should see your pipelines created there. You can click on your pipeline and press Debug to trigger a debug run of the pipeline. If at this stage you don't see your pipeline being created, most likely your `pipeline.json` configuration isn't correct. Review it and try again.

1. If necessary, add more pipelines under `/transform/pipelines` and/or in `config.transform.yaml` by repeating the above steps.

## Integrating data pipeline deployment into CI

To deploy your data pipeline as part of the CI deployment of your FlowEHR fork, do the following:

1. Update the link to your repository (or repositories) in `config.transform.yaml`, commit and push this change.

1. [Create a fine-grained personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) for your organisation with permissions to check out the data pipeline repositories you've created. Put it into a secret named `ORG_GH_TOKEN`. 

1. Run the `Deploy Infra-Test` action.
