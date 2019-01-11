CREATE TABLE `exchange` (
      `id`   int(5)     NOT NULL AUTO_INCREMENT,
      `name` varchar(50) NOT NULL,
	  PRIMARY KEY (id)
    ) ENGINE=MyISAM;


CREATE TABLE `market` (
  `id`          int(5)  NOT NULL AUTO_INCREMENT,
  `name`        char(50) NOT NULL,
  `id_exchange` int(5)  NOT NULL,
  PRIMARY KEY (id)
) ENGINE=MyISAM;

 CREATE INDEX id_exchange ON market (id_exchange);

CREATE TABLE `ticker` (
  `id`         int(11) NOT NULL AUTO_INCREMENT,
  `id_market`  int(5) NOT NULL,
  `local_time`  int(11)    NOT NULL,
  `timestamp`  int(11)    NOT NULL,
  `last`       DECIMAL (40,25) NOT NULL,
  volume	DECIMAL (40,25),
  `low`        DECIMAL (40,25),
  `high`       DECIMAL (40,25),
  `bid`        DECIMAL (40,25),
  `ask`        DECIMAL (40,25),
  PRIMARY KEY (id)
) ENGINE=MyISAM;

CREATE INDEX iarket ON ticker (id_market);

CREATE TABLE `exch_tick` (
  `id`         int(11) NOT NULL AUTO_INCREMENT,
  `id_market`  int(5) NOT NULL,
  `local_time`  int(14)    NOT NULL,
  `timestamp`  DECIMAL (40,25)    NOT NULL,
  `last`       DECIMAL (40,25) NOT NULL,
  volume		DECIMAL (40,25) NOT NULL,
  `low`        DECIMAL (40,25),
  `high`       DECIMAL (40,25),
  `bid`        DECIMAL (40,25),
  `ask`        DECIMAL (40,25),
  PRIMARY KEY (id)
) ENGINE=MyISAM;


CREATE TABLE `order_books` (
   `id`        int(11) NOT NULL AUTO_INCREMENT,
   `id_exchange` text,
   `local_time`  int(14)    NOT NULL,
`timestamp`  DECIMAL (40,25)    NOT NULL,
`id_market`  int(5) NOT NULL,
   `symbol`    text,
   `price`	   DECIMAL (40,25),
   `amount`    DECIMAL (40,25),
   bid		int(2) NOT NULL,
   PRIMARY KEY (id)
) ENGINE=MyISAM;



CREATE TABLE `fake_deals` (
  `id`         int(11) NOT NULL AUTO_INCREMENT,
  `id_market`  int(5) NOT NULL,
  `time_buy`  int(11)    NOT NULL,
  `time_sell`  int(11)    NOT NULL,
  `price_buy`       DECIMAL (40,25) NOT NULL,
  `price_sell`       DECIMAL (40,25) NOT NULL,
  `spread`        DECIMAL (40,25),
  PRIMARY KEY (id)
) ENGINE=MyISAM;


 INSERT INTO exch_tick (id_market, local_time, timestamp, last, low, high, bid, ask, volume) VALUES (24737, 1521914520.993629, 1521914487031, 0.060256, 0.060128, 0.061421, 0.060254, 0.060271, 15391.42)