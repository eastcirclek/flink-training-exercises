/*
 * Copyright 2017 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.dataset_preparation;

import com.dataartisans.flinktraining.examples.table_java.sources.TaxiRideTableSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import static org.apache.flink.api.common.typeinfo.Types.BOOLEAN;
import static org.apache.flink.api.common.typeinfo.Types.FLOAT;
import static org.apache.flink.api.common.typeinfo.Types.LONG;
import static org.apache.flink.api.common.typeinfo.Types.SHORT;
import static org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP;

public class TaxiRideKafkaProducer {

  public static void main(String[] args) throws Exception {

    // read parameters
    ParameterTool params = ParameterTool.fromArgs(args);
    String input = params.get("input", ExerciseBase.pathToRideData);
    String topic = params.get("topic", "nycTaxiRide");
    String bootstrapServers = params.get("bootstrapServers", "localhost:9092");

    final int maxEventDelay = 60;       // events are out of order by max 60 seconds
    final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // create a TableEnvironment
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    Table inputTable = tEnv.fromTableSource(new TaxiRideTableSource(input, maxEventDelay, servingSpeedFactor));

    ConnectorDescriptor connectorDescriptor = new Kafka()
      .version("universal")
      .topic(topic)
      .property("bootstrap.servers", bootstrapServers);

    FormatDescriptor formatDescriptor = new Csv()
      .deriveSchema()
      .fieldDelimiter(',');

    Schema schemaDescriptor = new Schema()
      .field("rideId", LONG)
      .field("taxiId", LONG)
      .field("driverId", LONG)
      .field("isStart", BOOLEAN)
      .field("startLon", FLOAT)
      .field("startLat", FLOAT)
      .field("endLon", FLOAT)
      .field("endLat", FLOAT)
      .field("passengerCnt", SHORT)
      .field("eventTime", SQL_TIMESTAMP);

    tEnv.connect(connectorDescriptor)
      .withFormat(formatDescriptor)
      .withSchema(schemaDescriptor)
      .inAppendMode()
      .registerTableSink("outputTable");

    tEnv.toAppendStream(inputTable, Row.class).print();
    inputTable.insertInto("outputTable");

    // execute query
    env.execute();
  }

}

