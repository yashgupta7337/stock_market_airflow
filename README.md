# Apache Airflow Practice Repository

This repository is dedicated to learning and practicing Apache Airflow. It includes examples, DAGs, and configurations for mastering workflows, task orchestration, and scheduling using Apache Airflow.

## Purpose
- Gain hands-on experience with Apache Airflow.
- Understand DAG (Directed Acyclic Graph) creation and task orchestration.
- Experiment with Astronomer CLI for local development.

---

## Prerequisites
- [Astronomer CLI](https://docs.astronomer.io/astro-cli) installed.
- Docker installed and running.

---

## Astro CLI Commands

The Astronomer CLI simplifies local development and management of Airflow projects. Below are the key commands:

- **Initialize an Airflow project**:
  ```bash
  astro dev init
  ```
- **Start the local Airflow environment:**:
  ```bash
  astro dev start
  ```
- **Restart the local Airflow environment:**:
  ```bash
  astro dev stop
  ```
- **Restart the local Airflow environment:**:
  ```bash
  astro dev restart
  ```
- **Login to the local Airflow CLI environment:**:
  ```bash
  astro dev bash
  ```

## Airflow CLI Commands

- **Test a specific Task of a DAG:**:
  ```bash
  airflow tasks test [dag_id] [task_id] [date]
  ```
