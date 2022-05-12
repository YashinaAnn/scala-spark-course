# Introduction to BigData (Scala+Spark): Marketing Analytics


## Task 1. Build Purchases Attribution Projection

### Configuration properties:
1. `clickstream-path` - path to input clickstream events dataset, default value = `"capstone-dataset/mobile_app_clickstream"`
2. `purchases-path` - path to input user purchases dataset, default value = `"capstone-dataset/user_purchases"`
3. `projection-path` - path to output projection dataset, default value = `"output_data/projectioh"`

### Task 1.1.
To build projection using Datasets API run:

```
sbt "runMain marketing_analyzer.projection.PurchaseProjectionDsBuilder"
```
or
```
spark-submit \
  --class marketing_analyzer.projection.PurchaseProjectionDsBuilder \
  --master local \
  --conf spark.driver.host=localhost \ 
  /target/scala-2.12/marketing_analyzer_2.12-0.1.jar
```

### Task 1.2.
To build projection using UDAF run:

```
sbt "runMain marketing_analyzer.projection.PurchaseProjectionUDAFBuilder"
```
or
```
spark-submit \
  --class marketing_analyzer.projection.PurchaseProjectionUDAFBuilder \
  --master local \
  --conf spark.driver.host=localhost \
  /target/scala-2.12/marketing_analyzer_2.12-0.1.jar
```

## Task 2. Calculate Marketing Campaigns And Channels Statistics

### Task 2.1. Top Campaigns

#### Configuration properties:
1. `projection-path` - path to input projection dataset, default value = `"output_data/projection"`
2. `top-campaigns-path` - path to output top campaigns dataset, default value = `"output_data/top-campaigns-path"`
3. `top-campaigns-limit` - top campaigns limit, default value=`10`


To calculate statistic using plain SQL on top of Spark DataFrame API run:
```
sbt "runMain marketing_analyzer.metrics.topCampaigns.TopCampaignsSqlCalculator"
```
or
```
spark-submit \
  --class marketing_analyzer.metrics.topCampaigns.TopCampaignsSqlCalculator \
  --master local \ 
  --conf spark.driver.host=localhost \ 
  /target/scala-2.12/marketing_analyzer_2.12-0.1.jar
```

To calculate statistic using Datasets API run:
```
sbt "runMain marketing_analyzer.metrics.topCampaigns.TopCampaignsDsCalculator"
```
or
```
spark-submit \
  --class marketing_analyzer.metrics.topCampaigns.TopCampaignsDsCalculator \
  --master local \
  --conf spark.driver.host=localhost \
  /target/scala-2.12/marketing_analyzer_2.12-0.1.jar
```

### Task 2.2. Channels engagement performance

#### Configuration properties:
1. `projection-path` - path to input projection dataset, default value = `"output_data/projection"`
2. `top-channels-path` - path to output top channels dataset, default value = `"output_data/top_channels"`


To calculate statistic using plain SQL on top of Spark DataFrame API run:
```
sbt "runMain marketing_analyzer.metrics.topChannels.TopChannelsDsCalculator"
```
or
```
spark-submit \
  --class marketing_analyzer.metrics.topChannels.TopChannelsDsCalculator \
  --master local \ 
  --conf spark.driver.host=localhost \ 
  /target/scala-2.12/marketing_analyzer_2.12-0.1.jar
```

To calculate statistic using Datasets API run:
```
sbt "runMain marketing_analyzer.metrics.topChannels.TopChannelsSqlCalculator"
```
or
```
spark-submit \
  --class marketing_analyzer.metrics.topChannels.TopChannelsSqlCalculator \
  --master local \
  --conf spark.driver.host=localhost \
  /target/scala-2.12/marketing_analyzer_2.12-0.1.jar
```

## Task 4. Build Pipeline using Apache Airflow
1. Start airflow webserver
```
airflow webserver
```
2. Move airflow/marketing_analytics_dag.py to the ~/airflow/dags directory
3. Add airflow variables
```
{
   "JAVA_HOME": "/path/to/java",
   "APP_HOME": "/path/to/project"
}
```
4. Start scheduler
```
airflow scheduler
```

Three tasks will be executed, results will be saved to the directories:
1. `build_projection` task - `projection-path` property
1. `top_campaigns` task - `top-campaigns-path` property
1. `top_channels` task - `top-channels-path` property