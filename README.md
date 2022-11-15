Make your first steps with Apache Kafka and Java
================================================


## Log in to Aiven platform

`avn user login <you@example.com> --token`

## Create Kafka Topic

`avn service topic-create kafka-sblanc customer-activity --partitions 3 --replication 2`

## Generate CA/Trust and config

`avn service user-kafka-java-creds kafka-sblanc --username avnadmin -d src/main/resources --password safePassword123`


[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/sebastienblanc/apache-kafka-first-steps-java/tree/gitpod)

This repository is an accompanying material for the blog post