CREATE TABLE `bike_usage_history` (
  `id` int AUTO_INCREMENT PRIMARY KEY,
  `lend_date` date,
  `lend_hour` int,
  `lend_station_id` int,
  `return_date` date,
  `return_hour` int,
  `return_station_id` int,
  `usage_time` int,
  `source_date` date,
  `create_time` timestamp default current_timestamp
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
  `create_time` timestamp default current_timestamp,
  `update_time` timestamp default current_timestamp
);

CREATE TABLE `bike_latest_update` (
  `bike_station_id` int PRIMARY KEY,
  `aval_bike` int,
  `aval_space` int,
  `source_time` timestamp,
  `create_time` timestamp default current_timestamp
);

CREATE TABLE `bike_realtime` (
  `bike_station_id` int,
  `aval_bike` int,
  `aval_space` int,
  `source_time` timestamp,
  `create_time` timestamp default current_timestamp,
  PRIMARY KEY (`bike_station_id`, `create_time`)
);


ALTER TABLE `bike_latest_update` ADD FOREIGN KEY (`bike_station_id`) REFERENCES `bike_station` (`bike_station_id`);

ALTER TABLE `bike_realtime` ADD FOREIGN KEY (`bike_station_id`) REFERENCES `bike_station` (`bike_station_id`);

ALTER TABLE `bike_usage_history` ADD FOREIGN KEY (`lend_station_id`) REFERENCES `bike_station` (`bike_station_id`);

ALTER TABLE `bike_usage_history` ADD FOREIGN KEY (`return_station_id`) REFERENCES `bike_station` (`bike_station_id`);


ALTER TABLE `bike_station` ADD FOREIGN KEY (`city_code`) REFERENCES `city` (`city_code`);




--需要先建立city (TPE) 才能完成bike DDL 的建立
INSERT INTO city (city_code, city_name) VALUES ('TPE', '台北市');
-- 插入一筆站名給對應不到站名時使用
INSERT INTO bike_station
	(bike_station_id, station_name) 
	VALUES ('-1', '對應不到bike站名');