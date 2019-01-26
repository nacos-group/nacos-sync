/******************************************/
/*   DB name = nacos_Sync   */
/*   Table name = cluster   */
/******************************************/
CREATE TABLE `cluster` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cluster_id` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `cluster_name` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `cluster_type` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `connect_key_list` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/******************************************/
/*   DB name = nacos_Sync   */
/*   Table name = system_config   */
/******************************************/
CREATE TABLE `system_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `config_desc` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `config_key` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `config_value` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/******************************************/
/*   DB name = nacos_Sync   */
/*   Table name = task   */
/******************************************/
CREATE TABLE `task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dest_cluster_id` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `group_name` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `name_space` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `operation_id` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `service_name` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `source_cluster_id` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `task_id` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `task_status` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `version` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `worker_ip` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
