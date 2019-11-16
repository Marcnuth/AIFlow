# AI Flow

## Introduction
AI Flow, which offers various reusable operators & processing units in AI modeling, helps AI engineer to write less, reuse more, integrate easily.

## Install

```
pip install aiflow
```


## Concepts

### Operators VS. Units

Ideally, we agree:
- An **Operator** would contain lot of units, which will be integrated into `airflow` for building non-realtime processing workflow;
- A **Unit** is a small calculation unit, which could be a function, or just a simple modeling logic, and it could be picked as bricks to build an operator. Besides, it could be reused anywhere for realtime calculation.

## Classes

### Operators

#### MongoToCSVOperator

#### Elastic2CSVOperator

#### RegExLabellingOperator

### Units

#### Doc2VecUnit

#### Doc2MatUnit

## Tests & Examples

### Example: Use Units to Build Your Castle


### Example: Working with Airflow

In `tests/docker/` folder, we provide examples on how to use `aiflow` with `airflow`.
It is a docker image, you could simply copy and start to use it!

In project root directory, run commands first:
```
docker-compose up --build aiflow
```

Then open `localhost:8080` in your browser, you can see all the examples `aiflow` provided!
Note: both the default username & password are `admin`

Enjoy!

## Contribution