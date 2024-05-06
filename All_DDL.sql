CREATE TABLE `bike_usage_history` (
  `id` int PRIMARY KEY,
  `lend_date` time,
  `lend_hour` int,
  `lend_station_id` int,
  `return_date` date,
  `reture_hour` int,
  `return_station_id` int,
  `usage_time` time,
  `source_date` date,
  `create_time` timestamp
);

CREATE TABLE `bike_station` (
  `bike_station_id` int PRIMARY KEY,
  `station_name` varchar(100),
  `total_space` int,
  `lat` decimal(8,6),
  `lng` decimal(9,6),
  `city_code` char(3),
  `district` varchar(20),
  `address` varchar(100),
  `disable` boolean,
  `create_time` timestamp,
  `update_time` timestamp
);

CREATE TABLE `bike_latest_update` (
  `bike_station_id` int PRIMARY KEY,
  `aval_bike` int,
  `aval_space` int,
  `source_time` timestamp,
  `create_time` timestamp
);

CREATE TABLE `bike_realtime` (
  `bike_station_id` int,
  `aval_bike` int,
  `aval_space` int,
  `source_time` timestamp,
  `create_time` timestamp,
  PRIMARY KEY (`bike_station_id`, `create_time`)
);

CREATE TABLE `bus_realtime_arrival` (
  `bus_route_id` int,
  `bus_stop_id` int,
  `direction` bool,
  `estimate_time` int,
  `source_time` timestamp,
  `create_time` timestamp,
  PRIMARY KEY (`bus_stop_id`, `direction`, `create_time`)
);

CREATE TABLE `bus_realtime_location` (
  `plate_number` varchar(10),
  `bus_route_id` int,
  `bus_subroute_id` int,
  `operator_id` int,
  `direction` int,
  `lat` decimal(8,6),
  `lng` decimal(9,6),
  `nearest_bus_stop_id` int,
  `speed` int,
  `duty_status` int,
  `bus_status` int,
  `source_time` timestamp,
  `create_time` timestamp,
  PRIMARY KEY (`plate_number`, `create_time`)
);

CREATE TABLE `bus_route` (
  `bus_route_id` int PRIMARY KEY,
  `route_name` varchar(20),
  `departure_stop_name` varchar(20),
  `destination_stop_name` varchar(20),
  `ticket_price` varchar(15),
  `city_code` char(3),
  `create_time` timestamp,
  `update_time` timestamp
);

CREATE TABLE `bus_subroute` (
  `bus_subroute_id` int PRIMARY KEY,
  `subroute_name` varchar(10),
  `bus_route_id` int,
  `direction` int,
  `first_bus_time` char(4),
  `last_bus_time` char(4),
  `holiday_first_bus_time` char(4),
  `holiday_last_bus_time` char(4),
  `create_time` timestamp,
  `update_time` timestamp
);

CREATE TABLE `bus_operator` (
  `operator_id` int PRIMARY KEY,
  `operator_name` varchar(20),
  `create_time` timestamp,
  `update_time` timestamp
);

CREATE TABLE `bus_route_stop_sequence` (
  `bus_subroute_id` int,
  `bus_stop_id` int,
  `stop_sequence` int,
  `direction` int,
  `create_time` timestamp,
  `update_time` timestamp,
  PRIMARY KEY (`bus_subroute_id`, `bus_stop_id`)
);

CREATE TABLE `bus_stop` (
  `bus_stop_id` int PRIMARY KEY,
  `bus_station_id` int,
  `stop_name` varchar(20),
  `create_time` timestamp,
  `update_time` timestamp
);

CREATE TABLE `bus_station` (
  `bus_station_id` int PRIMARY KEY,
  `station_name` varchar(20),
  `address` varchar(50),
  `city_code` char(3),
  `district` varchar(10),
  `lat` decimal(8,6),
  `lng` decimal(9,6),
  `bearing` char(1),
  `create_time` timestamp,
  `update_time` timestamp
);

CREATE TABLE `duty_status_detail` (
  `duty_status` int PRIMARY KEY,
  `description` varchar(20)
);

CREATE TABLE `bus_status_detail` (
  `bus_status` int PRIMARY KEY,
  `description` varchar(20)
);

CREATE TABLE `mrt_station` (
  `mrt_station_id` int PRIMARY KEY,
  `mrt_route_id` char(2),
  `sequence` char(2),
  `station_name` varchar(20),
  `station_en` varchar(100),
  `station_address` varchar(150),
  `lat` decimal(8,6),
  `lng` decimal(9,6),
  `city_code` char(3),
  `district` varchar(10),
  `bike_allow_on_holiday` bool,
  `create_time` timestamp,
  `update_time` timestamp
);

CREATE TABLE `mrt_usage_history` (
  `mrt_station_id` int PRIMARY KEY,
  `enter_count` int,
  `exit_count` int,
  `source_date` date,
  `source_hour` int,
  `create_time` timestamp
);

CREATE TABLE `mrt_realtime_arrival` (
  `mrt_station_id` int,
  `destination_id` varchar(10),
  `countdown` time,
  `source_time` timestamp,
  `create_time` timestamp,
  PRIMARY KEY (`mrt_station_id`, `destination_id`, `create_time`)
);

CREATE TABLE `mrt_realtime_crowded` (
  `mrt_station_id` int,
  `direction` int,
  `cart1` int,
  `cart2` int,
  `cart3` int,
  `cart4` int,
  `cart5` int,
  `cart6` int,
  `source_time` timestamp,
  `create_time` timestamp,
  PRIMARY KEY (`mrt_station_id`, `direction`, `create_time`)
);

CREATE TABLE `mrt_parking` (
  `mrt_ps_id` int PRIMARY KEY,
  `ps_name` varchar(20),
  `parking_type` char(2),
  `mrt_station_id` int,
  `park_total_no` int,
  `create_time` timestamp,
  `update_time` timestamp
);

CREATE TABLE `mrt_realtime_parking` (
  `mrt_parking_id` int,
  `park_now_no` int,
  `source_time` timestamp,
  `create_time` timestamp,
  PRIMARY KEY (`mrt_parking_id`, `create_time`)
);

CREATE TABLE `parking_station` (
  `ps_id` int PRIMARY KEY,
  `ps_name` varchar(200),
  `city_code` char(3),
  `district` varchar(50),
  `address` varchar(500),
  `total_space` int,
  `cost` varchar(50),
  `create_date` timestamp,
  `update_date` timestamp
);

CREATE TABLE `parking_realtime` (
  `ps_id` int PRIMARY KEY,
  `aval_space` int,
  `source_time` timestamp,
  `create_time` timestamp
);

CREATE TABLE `city` (
  `city_code` char(3) PRIMARY KEY,
  `city_name` char(3),
  `create_time` timestamp
);

ALTER TABLE `bike_latest_update` ADD FOREIGN KEY (`bike_station_id`) REFERENCES `bike_station` (`bike_station_id`);

ALTER TABLE `bike_realtime` ADD FOREIGN KEY (`bike_station_id`) REFERENCES `bike_station` (`bike_station_id`);

ALTER TABLE `bike_usage_history` ADD FOREIGN KEY (`lend_station_id`) REFERENCES `bike_station` (`bike_station_id`);

ALTER TABLE `bike_usage_history` ADD FOREIGN KEY (`return_station_id`) REFERENCES `bike_station` (`bike_station_id`);

ALTER TABLE `bus_realtime_location` ADD FOREIGN KEY (`operator_id`) REFERENCES `bus_operator` (`operator_id`);

ALTER TABLE `bus_subroute` ADD FOREIGN KEY (`bus_route_id`) REFERENCES `bus_route` (`bus_route_id`);

ALTER TABLE `bus_realtime_arrival` ADD FOREIGN KEY (`bus_route_id`) REFERENCES `bus_route` (`bus_route_id`);

ALTER TABLE `bus_stop` ADD FOREIGN KEY (`bus_station_id`) REFERENCES `bus_station` (`bus_station_id`);

ALTER TABLE `bus_route_stop_sequence` ADD FOREIGN KEY (`bus_stop_id`) REFERENCES `bus_stop` (`bus_stop_id`);

ALTER TABLE `bus_realtime_arrival` ADD FOREIGN KEY (`bus_stop_id`) REFERENCES `bus_stop` (`bus_stop_id`);

ALTER TABLE `bus_route_stop_sequence` ADD FOREIGN KEY (`bus_subroute_id`) REFERENCES `bus_subroute` (`bus_subroute_id`);

ALTER TABLE `bus_realtime_location` ADD FOREIGN KEY (`duty_status`) REFERENCES `duty_status_detail` (`duty_status`);

ALTER TABLE `bus_realtime_location` ADD FOREIGN KEY (`bus_status`) REFERENCES `bus_status_detail` (`bus_status`);

ALTER TABLE `mrt_usage_history` ADD FOREIGN KEY (`mrt_station_id`) REFERENCES `mrt_station` (`mrt_station_id`);

ALTER TABLE `mrt_parking` ADD FOREIGN KEY (`mrt_station_id`) REFERENCES `mrt_station` (`mrt_station_id`);

ALTER TABLE `mrt_realtime_parking` ADD FOREIGN KEY (`mrt_parking_id`) REFERENCES `mrt_parking` (`mrt_ps_id`);

ALTER TABLE `mrt_realtime_crowded` ADD FOREIGN KEY (`mrt_station_id`) REFERENCES `mrt_station` (`mrt_station_id`);

ALTER TABLE `mrt_realtime_arrival` ADD FOREIGN KEY (`mrt_station_id`) REFERENCES `mrt_station` (`mrt_station_id`);

ALTER TABLE `parking_station` ADD FOREIGN KEY (`ps_id`) REFERENCES `parking_realtime` (`ps_id`);

ALTER TABLE `parking_station` ADD FOREIGN KEY (`city_code`) REFERENCES `city` (`city_code`);

ALTER TABLE `bus_route` ADD FOREIGN KEY (`city_code`) REFERENCES `city` (`city_code`);

ALTER TABLE `mrt_station` ADD FOREIGN KEY (`city_code`) REFERENCES `city` (`city_code`);

ALTER TABLE `bus_station` ADD FOREIGN KEY (`city_code`) REFERENCES `city` (`city_code`);

ALTER TABLE `bike_station` ADD FOREIGN KEY (`city_code`) REFERENCES `city` (`city_code`);

