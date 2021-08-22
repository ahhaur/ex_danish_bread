# Assignment

## Assignment Information
**Github API**: [https://docs.github.com/en/free-pro-team@latest/rest](https://docs.github.com/en/free-pro-team@latest/rest)

**Target Repository**: [https://github.com/apache/airflow](https://github.com/apache/airflow)


With the data ingested, address the follow queries:
- For the ingested commits, determine the top 5 committers ranked by count of commits and their number of commits.
- For the ingested commits, determine the committer with the longest commit streak.
- For the ingested commits, generate a heatmap of number of commits count by all users by day of the week and by 3 hour blocks.



**Sample heatmap**：

|        | 00-03 | 03-06 | 06-09 | 09-12 | 12-15 | 15-18 | 18-21 | 21-00 |
|--------|-------|-------|-------|-------|-------|-------|-------|-------|
| Mon    |       |       |       |       |       |       |       |       |
| Tues   |       |       |       |       |       |       |       |       |
| Wed    |       |       |       |       |       |       |       |       |
| Thurs  |       |       |       |       |       |       |       |       |
| Fri    |       |       |       |       |       |       |       |       |
| Sat    |       |       |       |       |       |       |       |       |
| Sun    |       |       |       |       |       |       |       |       |


# Solution

## Setup Airflow & MariaDB instance in Docker

Start the instance under [docker folder](./docker/).
```shell
docker-compose up --build airflow-init & docker-compose up -d
```

Stop the instance and remove all the image
```shell
docker-compose down -v --rmi all
```

## Run in Airflow
In browser, open [http://localhost:8080/](http://localhost:8080/).

Username / Password: airflow / airflow


## Phpmyadmin for Mariadb
In browser, open [http://localhost:8081/](http://localhost:8081/).

Server: mariadb

Username / Password: root / S!mpl3P4ssw0rd

Database: edb


## Final Solutions
Please find [my solution](./sql/solutions.sql) in [sql folder](./sql/).


