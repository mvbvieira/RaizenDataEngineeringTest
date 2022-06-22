## RaizenDataEngineeringTest

# Installation 

Table of Contents
-----------------
  * [Requirements](#requirements)
  * [Installation](#installation)


Requirements
------------

Tools requires the following to run:

  * Docker
  * [docker-compose](docker-compose) 1.29.2

Installation
------------

Clone this repository and go to directory.

```bash
git clone git@github.com:mvbvieira/RaizenDataEngineeringTest.git
cd RaizenDataEngineeringTest/
```

Initialize the Database.

```bash
docker-compose up airflow-init
```

After returning this message: 

```bash
airflow-init_1       | Upgrades done
airflow-init_1       | Admin user airflow created
airflow-init_1       | 2.3.2
start_airflow-init_1 exited with code 0
```

Your envoirment was successed building.

The account created has the login `airflow` and the password `airflow`.

## Accessing the web interface

The webserver is available at: `http://localhost:8080`. The default account has the login `airflow` and the password `airflow`.

## Running already builded

```bash
docker-compose up
```
## Running the CLI commands

You can also use bash as parameter to enter interactive `bash` shell in the container or `python` to enter python container.

```bash
./airflow.sh bash
```

or 

```bash
./airflow.sh python
```

