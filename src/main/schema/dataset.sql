Reference URL --> http://www.infoobjects.com/spark-connecting-to-a-jdbc-data-source-using-dataframes/#

Step-1: Create table ‘person’ in MySql using following DDL
------------------------------------------------------------
CREATE TABLE `person` (
  `person_id` int(11) NOT NULL AUTO_INCREMENT,
  `first_name` varchar(30) DEFAULT NULL,
  `last_name` varchar(30) DEFAULT NULL,
  `gender` char(1) DEFAULT NULL,
  `age` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`person_id`)
)

step-2 : Insert sample data into table 'person'
-------------------------------------------------
Insert into person values(‘Barack’,’Obama’,’M’,53);
Insert into person values(‘Bill’,’Clinton’,’M’,71);
Insert into person values(‘Hillary’,’Clinton’,’F’,68);
Insert into person values(‘Bill’,’Gates’,’M’,69);
Insert into person values(‘Michelle’,’Obama’,’F’,51);


Step-3: Spark-Shell command
----------------------------
$spark-shell -–driver-class-path /path-to-mysql-jar/mysql-connector-java-5.1.34-bin.jar
