/*
Navicat MySQL Data Transfer

Source Server         : doitedu
Source Server Version : 50738
Source Host           : doitedu:3306
Source Database       : doit38

Target Server Type    : MYSQL
Target Server Version : 50738
File Encoding         : 65001

Date: 2023-06-16 18:31:18
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for model_meta
-- ----------------------------
DROP TABLE IF EXISTS `model_meta`;
CREATE TABLE `model_meta` (
  `model_id` varchar(11) NOT NULL,
  `calculator_code` mediumtext,
  PRIMARY KEY (`model_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
