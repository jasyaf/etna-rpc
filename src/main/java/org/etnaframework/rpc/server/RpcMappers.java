package org.etnaframework.rpc.server;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.etnaframework.core.logging.Log;
import org.etnaframework.core.spring.SpringContext;
import org.etnaframework.core.spring.annotation.OnContextInited;
import org.etnaframework.core.util.ReflectionTools;
import org.etnaframework.rpc.annotation.RpcService;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

/**
 * 初始化RPC容器，用于确定服务器端哪些Spring托管bean是可以远程调用的
 *
 * @author BlackCat
 * @since 2015-04-13
 */
@Service
public class RpcMappers {

    protected final Logger log = Log.getLogger();

    /** 保存方法签名到具体调用方法的映射关系 */
    private static Map<String, RpcMeta> map = Collections.emptyMap();

    /**
     * 遍历SpringContext，找出所有标注了{@link RpcService}的托管bean，初始化所有可远程调用的接口
     */
    @OnContextInited
    protected void init() throws Throwable {
        map = new LinkedHashMap<String, RpcMeta>();
        Collection<Object> beans = SpringContext.getBeansWithAnnotation(RpcService.class).values();
        for (Object serviceBean : beans) {
            Class<?> clazz = serviceBean.getClass();
            for (Class<?> intf : ReflectionTools.getAllInterfaces(clazz)) {
                // 屏蔽掉Spring框架相关的类，防止使用AOP等特性时出错
                if (intf.getName().startsWith("org.springframework")) {
                    continue;
                }
                for (Method interfaceMethod : intf.getMethods()) {
                    String signature = ReflectionTools.getMethodSingature(interfaceMethod);
                    Method implementMethod = serviceBean.getClass().getMethod(interfaceMethod.getName(), interfaceMethod.getParameterTypes());
                    RpcMeta meta = map.get(signature);
                    if (null != meta) {
                        throw new IllegalArgumentException(
                            "RPC接口方法" + signature + "已经被映射到实现" + ReflectionTools.getMethodSingature(meta.getMethod()) + "，无法再被映射到" + ReflectionTools.getMethodSingature(implementMethod));
                    }
                    meta = RpcMeta.create(serviceBean, interfaceMethod, implementMethod, signature);
                    map.put(signature, meta);
                }
            }
        }
    }

    /**
     * 根据接口方法签名查找对应的实现，如果没有将返回null
     */
    public RpcMeta getRpcMeta(String signature) {
        return map.get(signature);
    }

    private Map<String, RpcMeta> reverseRpcAllSortedMap;

    public Map<String, RpcMeta> getReverseRpcAllSortedMap() {
        if (reverseRpcAllSortedMap == null) {
            reverseRpcAllSortedMap = map;
        }
        return reverseRpcAllSortedMap;
    }
}
