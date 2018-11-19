package com.alibaba.nacossync.extension;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacossync.pojo.model.TaskDO;

public interface SyncService {
	
	public boolean delete(TaskDO taskDO) throws NacosException;
	
	public boolean sync(TaskDO taskDO);
	 

}
