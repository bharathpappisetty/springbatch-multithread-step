/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.spring.batch.partitiondemo.configuration;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.task.batch.partition.DeployerPartitionHandler;
import org.springframework.cloud.task.batch.partition.DeployerStepExecutionHandler;
import org.springframework.cloud.task.batch.partition.PassThroughCommandLineArgsProvider;
import org.springframework.cloud.task.batch.partition.SimpleEnvironmentVariablesProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;

import io.spring.batch.partitiondemo.domain.Device;

/**
 * @author Michael Minella
 */
@Configuration
public class BatchConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private JobRepository jobRepository;

	@Autowired
	private ConfigurableApplicationContext context;
	
	@Value("${max.workers}")
	private int maxWorkers;
	
	@Value("${worker.app.jar}")
	private Resource resource;

	
	@Bean
	public DeployerPartitionHandler partitionHandler(TaskLauncher taskLauncher,
			JobExplorer jobExplorer,
			ApplicationContext context,
			Environment environment) throws Exception {
		Resource resource = context.getResource("file:D:/WinathonSpace/scaling-demos-master2/scaling-demos-master/partitioned-demo/target/partitioned-demo-0.0.1-SNAPSHOT.jar");

		DeployerPartitionHandler partitionHandler = new DeployerPartitionHandler(taskLauncher, jobExplorer, resource, "step1");

		List<String> commandLineArgs = new ArrayList<>(3);
		commandLineArgs.add("--spring.profiles.active=worker");
		commandLineArgs.add("--spring.cloud.task.initialize.enable=false");
		commandLineArgs.add("--spring.batch.initializer.enabled=false");
		commandLineArgs.add("--spring.datasource.initialize=false");
		partitionHandler.setCommandLineArgsProvider(new PassThroughCommandLineArgsProvider(commandLineArgs));
		partitionHandler.setEnvironmentVariablesProvider(new SimpleEnvironmentVariablesProvider(environment));
		String maxWorkerStr = environment.getProperty("max.workers");
		int maxWorkers = 0;
		
		try {
			maxWorkers = Integer.parseInt(maxWorkerStr);
		} catch (Exception e) {}
		
		partitionHandler.setMaxWorkers(maxWorkers);
		partitionHandler.setApplicationName("PartitionedBatchJobTask");
		return partitionHandler;
	}

	@Bean
	@StepScope
	public MultiResourcePartitioner partitioner(@Value("#{jobParameters['inputFiles']}") Resource[] resources) {
		MultiResourcePartitioner partitioner = new MultiResourcePartitioner();

		partitioner.setKeyName("file");
		partitioner.setResources(resources);

		return partitioner;
	}

	@Bean
	@Profile("worker")
	public DeployerStepExecutionHandler stepExecutionHandler(JobExplorer jobExplorer) {
		return new DeployerStepExecutionHandler(this.context, jobExplorer, this.jobRepository);
	}

	@Bean
	@StepScope
	public FlatFileItemReader<Device> fileTransactionReader(
			@Value("#{stepExecutionContext['file']}") Resource resource) {

		return new FlatFileItemReaderBuilder<Device>()
				.name("flatFileTransactionReader")
				.resource(resource)
				.delimited()
				.names(new String[] { "username", "userid", "devicename", "datetime", "parameter", "metricvalue",  "location" })
				.fieldSetMapper(fieldSet -> {
					Device device = new Device();

					device.setUsername(fieldSet.readString("username"));
					device.setUserid(fieldSet.readString("userid"));
					device.setDevicename(fieldSet.readString("devicename"));
					device.setDatetime(fieldSet.readString("datetime"));
					device.setParameter(fieldSet.readString("parameter"));
					device.setMetricvalue(fieldSet.readString("metricvalue"));
					device.setLocation(fieldSet.readString("location"));
					return device;
				})
				.build();
	}

/*	
	USERNAME VARCHAR(250) NOT NULL ,
	USERID VARCHAR(250) NOT NULL ,
	DEVICENAME VARCHAR(250) NOT NULL ,
	DATETIME VARCHAR(250) NOT NULL ,
	PARAMETER VARCHAR(250) NOT NULL ,
	METRICVALUE VARCHAR(250) NOT NULL ,
	LOCATION VARCHAR(250) NOT NULL */
	@Bean
	@StepScope
	public JdbcBatchItemWriter<Device> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Device>()
				.dataSource(dataSource)
				.beanMapped()
				.sql("INSERT INTO DEVICE (USERNAME, USERID, DEVICENAME,DATETIME,PARAMETER,METRICVALUE,LOCATION) VALUES"
						+ " (:username, :userid, :devicename, :datetime, :parameter, :metricvalue, :location)")
				.build();
	}

	@Bean
	public Step partitionedMaster(PartitionHandler partitionHandler) {
		return this.stepBuilderFactory.get("step1")
				.partitioner(step1().getName(), partitioner(null))
				.step(step1())
				.partitionHandler(partitionHandler)
				.build();
	}

	@Bean
	public Step step1() {
		return this.stepBuilderFactory.get("step1")
				.<Device, Device>chunk(5000)
				.reader(fileTransactionReader(null))
				.writer(writer(null))
				.build();
	}

	@Bean
	@StepScope
	public MultiResourceItemReader<Device> multiResourceItemReader(
			@Value("#{jobParameters['inputFiles']}") Resource[] resources) {

		return new MultiResourceItemReaderBuilder<Device>()
				.delegate(delegate())
				.name("multiresourceReader")
				.resources(resources)
				.build();
	}

	@Bean
	public FlatFileItemReader<Device> delegate() {
		return new FlatFileItemReaderBuilder<Device>()
				.name("flatFileTransactionReader")
				.delimited()
				.names(new String[] { "username", "userid", "devicename", "datetime", "parameter", "metricvalue",  "location" })
				.fieldSetMapper(fieldSet -> {
					Device device = new Device();

					device.setUsername(fieldSet.readString("username"));
					device.setUserid(fieldSet.readString("userid"));
					device.setDevicename(fieldSet.readString("devicename"));
					device.setDatetime(fieldSet.readString("datetime"));
					device.setParameter(fieldSet.readString("parameter"));
					device.setMetricvalue(fieldSet.readString("metricvalue"));
					device.setLocation(fieldSet.readString("location"));
					return device;
				})
				.build();
	}

	@Bean
	@Profile("!worker")
	public Job parallelStepsJob() {
		return this.jobBuilderFactory.get("parallelStepsJob")
				.start(partitionedMaster(null))
				.build();
	}
}
