# Pipeline to extract TLC DATA

The city of New York provides historical data of "The New York City Taxi and Limousine Commission" (TLC). Your colleagues from the data science team want to create various evaluations and predictions based on this data. Depending on their different use cases, they need the output data in a row-oriented and column-oriented format. So they approach to you and ask for your help. Your colleagues only rely on a frequent output of the datasets in these two formats, so you have a free choice of your specific technologies.

 ##  About TLC
The New York City Taxi and Limousine Commission (TLC), created in 1971, is the agency responsible for licensing and regulating New York City's Medallion (Yellow) taxi cabs, for-hire vehicles (community-based liveries, black cars and luxury limousines), commuter vans, and paratransit vehicles. The Commission's Board consists of nine members, eight of whom are unsalaried Commissioners. The salaried Chair/ Commissioner presides over regularly scheduled public commission meetings and is the head of the agency, which maintains a staff of approximately 600 TLC employees.
Over 200,000 TLC licensees complete approximately 1,000,000 trips each day. To operate for hire, drivers must first undergo a background check, have a safe driving record, and complete 24 hours of driver training. TLC-licensed vehicles are inspected for safety and emissions at TLC's Woodside Inspection Facility.


  ## Pipeline
 This pipeline will extract csv files from the [Data Source](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and transform and extract to parquet file format. Each file contains one month of Data for different years 2009-2021.
 We use AirFlow in a Docker container to schedule the data pipeline monthly.


### Before you begin
1.  Install  [Docker Community Edition (CE)](https://docs.docker.com/engine/installation/)  on your workstation. Depending on the OS, you may need to configure your Docker instance to use 4.00 GB of memory for all containers to run properly. Please refer to the Resources section if using  [Docker for Windows](https://docs.docker.com/docker-for-windows/#resources)  or  [Docker for Mac](https://docs.docker.com/docker-for-mac/#resources)  for more information.
2.  Install  [Docker Compose](https://docs.docker.com/compose/install/)  v1.29.1 and newer on your workstation.
3. Clone the repository in your directory

        git clone git@github.com:AlirezaHabibi2010/Pipeline-to-Extract-TLC-DATA.git
        cd Pipeline-to-Extract-TLC-DATA

 ### Running Airflow
It's time to create and docker container with airflow. All container information exists in the the file named *'docker-compose.yaml'*. First go to the repository folder and run the following commands to initiate the container.


    mkdir ./data ./logs ./plugins
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    docker-compose up airflow-init
    docker-compose -f docker-compose.yaml up -d
    docker exec -it taxi-statistic-airflow-worker-1 \
	    pip install requests numpy pandas pyarrow

Now the Docker container is up in the background and some extra libraries are installed inside it. To check the Docker's status use the following command.

    docker ps

The main container which you can use for Airflow commands is named *'airflow-pipeline-airflow-worker-1'*. The Airflow web interface can be accessed by [localhost:8080](http://localhost:8080/home). Pipeline codes are in the *'dags'* folder. *'csv_convert_dag'* is our pipeline's name.

### Prevent auto starting the pipeline
With following command you can initiate the pipeline. *'csv_convert_dag'* is our pipeline's name.

    docker exec -it taxi-statistic-airflow-worker-1 airflow dags  unpause csv_convert_dag

In the first run, It runs the pipeline from '2018-08-25' up to now. Then It schedule the run monthly.

Alternatively, If you like to run the pipeline between two different dates, you can use the following command. Do not forger to replace the start and end date.

    docker exec -it taxi-statistic-airflow-worker-1 airflow dags backfill csv_convert_dag -s 2018-08-28 -e 2021-07-28

 After a successful run, All parquet files will be stored in *'./data/green_par'* and *'./data/yellow_par'* for green and yellow taxis.

### Stop the container
To stop the container:

	docker-composer down

### delete the container
 To delete all files related to the container except the repository files and data folder.

	docker-compose down --volumes --rmi all

### start PySpark
 After triggering the pipeline, you have a the parquet data in folders *'./data/green_par'* and *'./data/yellow_par'*.
 Using jupyter in *'taxi_spark.ipynb*', you can merge parquet files and transform them to avra format. For do so, you should install  spark.

 To check the data, there are some query and explanation in the *'taxi_spark.ipynb*' file.
