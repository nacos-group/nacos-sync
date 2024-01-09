package com.alibaba.nacossync.pojo.request;

import com.alibaba.nacossync.pojo.view.ServiceView;
import lombok.Data;

import java.util.List;

/**
 * @author wyd
 * @since 2024-01-04 15:54:20
 */
@Data
public class CatalogServiceResult {
    /**
     * count，not equal serviceList.size .
     */
    private int count;

    private List<ServiceView> serviceList;
}
