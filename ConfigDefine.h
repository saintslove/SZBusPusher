/*
 * ConfigDefine.h
 *
 *  Created on: 2016年12月7日
 *      Author: wong
 */

#ifndef CONFIGDEFINE_H_
#define CONFIGDEFINE_H_

// 配置文件的路径
#define CONFIG_PATH "./SZBusPusher.conf"

// 配置文件中用到的key值定义
#define LOG_LEVEL   "log_level"     // 日志级别
#define LOG_PATH    "log_path"      // 日志路径
#define DEVSN       "devsn"         // 设备SN
#define WHITE_LIST  "white_list"    // 白名单
#define LISTEN_IP_INT   "listen_ip_int"     // 本地监听IP for 接入
#define LISTEN_PORT_INT "listen_port_int"   // 本地监听端口 for 接入
#define LISTEN_IP_EXT   "listen_ip_ext"     // 本地监听IP for 外部
#define LISTEN_PORT_EXT "listen_port_ext"   // 本地监听端口 for 外部
#define KAFKA_VERSION   "kafka_version"     // kafka版本号
#define KAFKA_BROKERS   "kafka_brokers"     // kafka broker列表，形如"ip1:port1,ip2:port2,..."
#define KAFKA_TOPIC     "kafka_topic"       // kafka topic

#endif /* CONFIGDEFINE_H_ */
