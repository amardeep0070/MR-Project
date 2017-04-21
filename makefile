# Makefile for Classification/Prediction project.
# The structure of the input file in aws looks as follows:
#input/
#├── ModelConfigurations
#│   └── parameters ---> parameters for different models to be used for training
#├── PreProcessorInput
#│   ├── Labeled
#│   │   └── labeled.csv.bz2
#│   └── Unlabeled
#│       └── unlabeled.csv.bz2
#└── columnNames
#    └── columns ---> a single list of all the column names present in labeled.csv.bz2
# Customize these paths for your environment.
# -----------------------------------------------------------
#hadoop.root=/usr/bin/hadoop/hadoop-2.7.3
hadoop.root=/usr/local/Cellar/hadoop/2.7.3
jar.name=classfier-project.jar
local.jar.name=classifier-local-project.jar
#maven.jar.name=BirdClassifier-1.0-beta-jar-with-dependencies.jar
maven.local.jar.name=BirdClassifier-1.0-beta.jar
job.name=Driver
local.input=input
local.output=output
local.log.dir=projectTrainLog
# Pseudo-Cluster Execution
hdfs.user.name=joe
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-5.2.1
aws.region=us-east-1
aws.bucket.name=amardeep-mr
aws.subnet.id=subnet-e91e62b2
aws.input=inputProject
aws.output=outputClassifierBig
aws.log.dir=log
aws.num.nodes=5
aws.instance.type=m4.large
# Change the run type based on training/prediction Values: {train,predict}

# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package
	cp target/${maven.local.jar.name} ${jar.name}
	cp target/${maven.local.jar.name} ${local.jar.name}
	zip -d ${local.jar.name} META-INF/LICENSE

jar-old-school:
	rm -rf scratch
	mkdir scratch
	javac -d scratch -cp src:${hadoop.root}/share/hadoop/common/hadoop-common-2.7.3.jar:${hadoop.root}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.3.jar \
			$(shell find ./com/neu/cs6240/* | grep .java)
	jar cf ${jar.name} -C scratch weather
	rm -r scratch

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}

# Removes local log directory.
clean-local-log:
	rm -rf ${local.log.dir}

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${local.jar.name} ${job.name} ${run.type} ${local.input} ${local.output}

# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${hadoop.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}

# Download output from HDFS to local.
download-output:
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	${hadoop.root}/bin/hadoop jar ${jar.name} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	${hadoop.root}/bin/hadoop jar ${jar.name} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/${aws.output} --recursive

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.name} s3://${aws.bucket.name}

# Main EMR launch.
cloud: jar upload-app-aws
	aws emr create-cluster \
		--name "ClassifierProjectTrain3" \
		--release-label emr-5.0.0 \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}

# Download log from S3.
download-log-aws: clean-local-log
	mkdir ${local.log.dir}
	aws s3 sync s3://${aws.bucket.name}/${aws.log.dir} ${local.log.dir}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -rf build
	mkdir build
	mkdir build/deliv
	mkdir build/deliv/WeatherJ
	cp pom.xml build/deliv/WeatherJ
	cp -r src build/deliv/WeatherJ
	cp Makefile build/deliv/WeatherJ
	cp README.txt build/deliv/WeatherJ
	tar -czf WeatherJ.tar.gz -C build/deliv WeatherJ
	cd build/deliv && zip -rq ../../WeatherJ.zip WeatherJ
