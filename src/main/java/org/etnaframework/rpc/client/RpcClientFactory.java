package org.etnaframework.rpc.client;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.etnaframework.core.logging.Log;
import org.etnaframework.core.spring.SpringContext;
import org.etnaframework.core.spring.annotation.Config;
import org.etnaframework.core.util.DatetimeUtils.Datetime;
import org.etnaframework.core.util.NetUtils;
import org.etnaframework.core.util.ReflectionTools;
import org.etnaframework.core.util.ThreadUtils;
import org.etnaframework.rpc.annotation.RpcService;
import org.etnaframework.rpc.codec.RpcRequest;
import org.etnaframework.rpc.codec.RpcResponse;
import org.etnaframework.rpc.exception.RpcConnectToServerException;
import org.etnaframework.rpc.server.RpcServer;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

/**
 * RPC服务客户端工厂类，请使用{@link #getInstance(Class, String, int)}方法获取对应的接口的远程调用实现
 *
 * @author BlackCat
 * @since 2015-04-18
 */
@Service
public final class RpcClientFactory {

    protected final static Logger log = Log.getLogger();

    /** 关键字到RPC服务代理实现的映射 */
    private final static Map<String, Object> serviceCache = new ConcurrentHashMap<String, Object>();

    /**
     * 获取RPC服务接口的实现
     *
     * @param interfaceClass 服务接口定义类
     * @param host 远程地址
     * @param port 远程地址监听端口
     */
    public static <T> T getInstance(Class<T> interfaceClass, String host, int port) {
        String key = interfaceClass.getName() + "@" + host + ":" + port;
        if (serviceCache.containsKey(key)) {
            return interfaceClass.cast(serviceCache.get(key));
        }
        synchronized (serviceCache) {
            T t = interfaceClass.cast(serviceCache.get(key));
            if (t == null) {
                if (!interfaceClass.isInterface()) {
                    throw new IllegalArgumentException(interfaceClass.getName() + "必须是interface");
                }
                // 检测是否是当前进程提供的服务，如果是的话就直接调用了，不走网络通信
                String remoteIP = NetUtils.getIP(new InetSocketAddress(host, port));
                if (NetUtils.getLocalIPWith127001().contains(remoteIP)) { // 如果IP相同，接下来比对端口是否相同
                    boolean local = false;
                    Collection<RpcServer> list = SpringContext.getBeansOfType(RpcServer.class).values();
                    for (RpcServer rs : list) {
                        for (InetSocketAddress addr : rs.getPorts()) {
                            if (addr.getPort() == port) {
                                local = true;
                                break;
                            }
                        }
                    }
                    // 端口能对上了，再看本地有没有对应的实现
                    if (local) {
                        t = SpringContext.getBean(interfaceClass);
                        if (null != t) {
                            // 检查有没加注解，没加的话远程调用会出错，本地调用时也需要检查一下
                            if (null == t.getClass().getAnnotation(RpcService.class)) {
                                throw new IllegalArgumentException("类" + t.getClass().getName() + "上需要加@" + RpcService.class.getSimpleName() + "才能被RPC远程调用");
                            }
                            log.debug("{} is local, RPC Disabled", key);
                            serviceCache.put(key, t);
                            return t;
                        }
                    }
                }
                // 确定本地没有，就使用远程的服务
                t = interfaceClass.cast(Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class<?>[] {
                    interfaceClass
                }, new RpcInterfaceProxyHandler(host, port)));
                serviceCache.put(key, t);
            }
            return t;
        }
    }

    /** 方法到方法签名的映射，缓存起来减少重复运算 */
    private static Map<Method, String> method2signature = new ConcurrentHashMap<Method, String>();

    /** 当请求远程服务器失败时，客户端最多重试的次数 */
    @Config(value = "etna.rpc.client.maxRetryTime", resetable = false)
    private static int maxRetryTime = 1;

    /** 发送不成功时的重试间隔，单位为毫秒 */
    @Config(value = "etna.rpc.client.retryIdleMs", resetable = false)
    private static long retryIdleMs = Datetime.MILLIS_PER_SECOND;

    /**
     * 客户端通过接口生成代理实例的工具类，用于返回接口实例，内部通过网络访问远程服务器的资源
     */
    static class RpcInterfaceProxyHandler implements InvocationHandler {

        private String host;

        private int port;

        public RpcInterfaceProxyHandler(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String signature = method2signature.get(method);
            if (null == signature) {
                synchronized (method2signature) {
                    signature = method2signature.get(method);
                    if (null == signature) {
                        signature = ReflectionTools.getMethodSingature(method);
                        method2signature.put(method, signature);
                    }
                }
            }
            RpcRequest req = new RpcRequest(signature, args);
            Object result = null;
            for (int i = 0; i <= maxRetryTime; i++) {
                try {
                    RpcClient client = RpcClient.getInstance(host, port);
                    RpcResponse resp = client.send(req);
                    result = resp.result;
                    break;
                } catch (IOException ex) {
                    // 如果是刚好重启的那一瞬间导致连接断开，重试一下
                    // 否则就直接把异常抛出去，如果是重试的最后一次，也要将异常抛出去
                    if (!ex.getMessage().contains("Connection reset by peer") || i == maxRetryTime) {
                        throw ex;
                    }
                    ThreadUtils.sleep(retryIdleMs);
                } catch (RpcConnectToServerException ex) {
                    // 连不上远程服务器，重试，最后一次将异常抛出去
                    if (i == maxRetryTime) {
                        throw ex;
                    }
                    ThreadUtils.sleep(retryIdleMs);
                }
            }
            return result;
        }
    }
}
