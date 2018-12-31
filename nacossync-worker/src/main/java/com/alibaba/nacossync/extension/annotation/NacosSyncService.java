package com.alibaba.nacossync.extension.annotation;

import com.alibaba.nacossync.constant.ClusterTypeEnum;
import org.springframework.core.annotation.AliasFor;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * 同步服务实现类注解
 * 
 * @author paderlol
 * @date: 2018-12-31 15:28
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface NacosSyncService {

    @AliasFor(annotation = Component.class)
    String value() default "";

    ClusterTypeEnum clusterType();
}
