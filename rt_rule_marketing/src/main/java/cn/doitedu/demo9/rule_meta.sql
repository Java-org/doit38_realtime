/*
Navicat MySQL Data Transfer

Source Server         : doitedu
Source Server Version : 50738
Source Host           : doitedu:3306
Source Database       : doit38

Target Server Type    : MYSQL
Target Server Version : 50738
File Encoding         : 65001

Date: 2023-06-16 14:12:57
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for rule_meta
-- ----------------------------
DROP TABLE IF EXISTS `rule_meta`;
CREATE TABLE `rule_meta` (
  `rule_id` varchar(255) NOT NULL,
  `rule_model_id` varchar(255) DEFAULT NULL,
  `rule_param_json` text,
  `online_status` int(11) DEFAULT NULL,
  `pre_select_crowd` mediumblob,
  `dynamic_profile_history_end_time` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`rule_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
