#!/bin/bash

# hive, aws, iceberg
echo """
spark.hadoop.hive.metastore.uris                            $HIVE_METASTORE_URIS
spark.sql.warehouse.dir                                     $HIVE_WAREHOUSE_DIR
spark.hadoop.fs.s3a.endpoint                                $AWS_ENDPOINT
spark.hadoop.fs.s3a.path.style.access                       true
spark.hadoop.fs.s3a.ssl.enabled                             false
spark.hadoop.fs.s3a.impl                                    org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider                com.amazonaws.auth.EnvironmentVariableCredentialsProvider

spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version    2

spark.sql.extensions                                        org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog                             org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type                        hive
spark.sql.catalog.spark_catalog.metrics-reporter-impl       org.apache.iceberg.metrics.LoggingMetricsReporter

spark.sql.catalog.default_iceberg                           org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.default_iceberg.type                      hive
spark.sql.catalog.default_iceberg.uri                       $HIVE_METASTORE_URIS
spark.sql.catalog.default_iceberg.io-impl                   org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.default_iceberg.s3.endpoint               $AWS_ENDPOINT
spark.sql.catalog.default_iceberg.warehouse                 $HIVE_WAREHOUSE_DIR
spark.sql.catalog.default_iceberg.metrics-reporter-impl     org.apache.iceberg.metrics.LoggingMetricsReporter

spark.sql.defaultCatalog                                    spark_catalog

""" >> $SPARK_CONF_DIR/spark-defaults.conf

# compute resources
echo """
spark.dynamicAllocation.enabled                             true
spark.driver.maxResultSize                                  0
spark.executor.cores                                        1
spark.executor.memory                                       1g
spark.executor.instances                                    1
spark.dynamicAllocation.minExecutors                        1
spark.dynamicAllocation.maxExecutors                        2

""" >> $SPARK_CONF_DIR/spark-defaults.conf

# kubernetes
echo """
spark.master                                                k8s://https://kubernetes.default.svc.cluster.local:443
spark.submit.deployMode                                     client

spark.kubernetes.driver.master                              https://kubernetes.default.svc.cluster.local:443
spark.kubernetes.namespace                                  $K8S_NAMESPACE
spark.kubernetes.authenticate.serviceAccountName            $K8S_SERVICE_ACCOUNT
spark.kubernetes.authenticate.driver.serviceAccountName     $K8S_SERVICE_ACCOUNT
spark.kubernetes.container.image                            $K8S_CONTAINER_IMAGE
spark.kubernetes.container.image.pullPolicy                 Always

spark.driver.extraJavaOptions                               -Divy.cache.dir=/tmp -Divy.home=/tmp
spark.driver.host                                           $MY_POD_IP
spark.driver.port                                           20000

spark.kubernetes.driver.pod.name                            $MY_POD_NAME
spark.kubernetes.executor.podNamePrefix                     $MY_POD_NAME
spark.kubernetes.driver.service.deleteOnTermination         true
spark.kubernetes.executor.deleteOnTermination               true

""" >> $SPARK_CONF_DIR/spark-defaults.conf

if [ "$K8S_SECRET_REF" ]; then

echo """
spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID          $K8S_SECRET_REF:AWS_ACCESS_KEY_ID
spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY      $K8S_SECRET_REF:AWS_SECRET_ACCESS_KEY
spark.kubernetes.driver.secretKeyRef.AWS_REGION                 $K8S_SECRET_REF:AWS_REGION
spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID        $K8S_SECRET_REF:AWS_ACCESS_KEY_ID
spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY    $K8S_SECRET_REF:AWS_SECRET_ACCESS_KEY
spark.kubernetes.executor.secretKeyRef.AWS_REGION               $K8S_SECRET_REF:AWS_REGION

""" >> $SPARK_CONF_DIR/spark-defaults.conf

fi
