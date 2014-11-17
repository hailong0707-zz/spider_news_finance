-- mysql>source db.sql
-- DROP语句慎用，第一次创建数据库的时候请取消注释
-- DROP DATABASE IF EXISTS news;
CREATE DATABASE news;
USE news;
CREATE TABLE news_finance(
	id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
	web VARCHAR(255) NOT NULL,
	type1 VARCHAR(255) NOT NULL,
	type2 VARCHAR(255) NOT NULL,
	day VARCHAR(255) NOT NULL,
	time VARCHAR(255) NOT NULL,
	title VARCHAR(255) NOT NULL,
	tags VARCHAR(255),
	article TEXT NOT NULL
);
CREATE TABLE news_gov(
    id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
    gov_name VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    title VARCHAR(255) NOT NULL,
    day VARCHAR(255) NOT NULL,
    year VARCHAR(255) NOT NULL,
    num VARCHAR(255),
    key_words VARCHAR(255),
    article LONGTEXT,
    gov_others VARCHAR(255),
    attachments TEXT,
    attachments_content LONGTEXT
);