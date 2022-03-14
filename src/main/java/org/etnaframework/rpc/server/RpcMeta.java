package org.etnaframework.rpc.server;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.etnaframework.core.logging.Log;
import org.etnaframework.core.util.ReflectionTools;
import org.etnaframework.core.web.mapper.CmdMappers.StageTimeSpanStat;
import org.etnaframework.rpc.codec.RpcRequest;
import org.slf4j.Logger;
import com.alibaba.fastjson.annotation.JSONField;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtNewMethod;

/**
 * <pre>
 * 保存接口的方法签名到远程调用bean和方法的映射关系
 * 方法签名是指的{@link RpcRequest#signature}，服务器端将根据方法签名寻找到对应的实现类
 * 这里是单个的方法的映射
 * </pre>
 *
 * @author BlackCat
 * @since 2014-04-14
 */
public abstract class RpcMeta {

    protected static final Logger log = Log.getLogger();

    private static CtClass rpcMetaCtClass = ReflectionTools.getCtClass(RpcMeta.class);

    /**
     * <pre>
     * 经测试发现，在另一个classloader加载的类是无法处理其他classloader里面的类的，会报错frozen class (cannot edit)
     * 为了增加etna健壮性，当无法使用javassist方式生成类时，就使用传统的反射方式来实现
     * </pre>
     */
    private static boolean useReflect = false;

    /** 接口调用统计 */
    private StageTimeSpanStat stat;

    /** 方法签名，包含接口类名、方法名、参数类型等，是可以唯一标识一个方法的字符串 */
    private String signature;

    /** 被映射到的接口实现方法，用于判断去重，防止出现多个实现导致服务器不知要调用哪个的问题 */
    @JSONField(serialize = false, deserialize = false) // 防止调用/stat/rpc接口时打入access日志里面去了，冗余信息太多，不需要
    private Method method;

    /**
     * 执行对应的远程方法并获取返回结果
     */
    public abstract Object invoke(Object[] args) throws Throwable;

    /**
     * 生成调用指定远程方法的{@link RpcMeta}
     */
    static RpcMeta create(final Object serviceBean, final Method interfaceMethod, Method implementMethod, String interfaceSignature) throws Throwable {
        if (!useReflect) {
            try {
                ClassPool pool = rpcMetaCtClass.getClassPool();
                String genClassName = interfaceMethod.getDeclaringClass().getName() + "." + interfaceMethod.getName() + "." + rpcMetaCtClass.getSimpleName(); // 生成的class名称，使用interface.method.RpcMeta来命名
                CtClass mc = pool.makeClass(genClassName);
                mc.setSuperclass(rpcMetaCtClass);
                // 增加一个对serviceBean的引用，方便在invoke中调用
                String src = "private " + serviceBean.getClass().getName() + " service;";
                mc.addField(CtField.make(src, mc));
                src = "public Object invoke(" + Object[].class.getName() + " args) throws Throwable {" + "return service." + interfaceMethod.getName() + "(args);" + "}";
                mc.addMethod(CtNewMethod.make(src, mc));
                // 实例化，然后通过反射将serviceBean引用传入进去
                RpcMeta cm = (RpcMeta) mc.toClass().newInstance();
                Field f = cm.getClass().getDeclaredField("service");
                f.setAccessible(true);
                f.set(cm, serviceBean);
                cm.signature = interfaceSignature;
                cm.method = implementMethod;
                cm.resetCounter(interfaceSignature);
                return cm;
            } catch (Throwable re) {
                log.error("javassist cannot create, use reflection instead");
                useReflect = true;
            }
        }
        RpcMeta cm = new RpcMeta() {

            @Override
            public Object invoke(Object[] args) throws Throwable {
                return interfaceMethod.invoke(serviceBean, args);
            }
        };
        cm.signature = interfaceSignature;
        cm.method = implementMethod;
        cm.resetCounter(interfaceSignature);
        return cm;
    }

    public StageTimeSpanStat getStat() {
        return stat;
    }

    public void resetCounter(String name) {
        this.stat = new StageTimeSpanStat(name);
    }

    public void setStat(StageTimeSpanStat stat) {
        this.stat = stat;
    }

    public String getName() {
        return signature;
    }

    public Method getMethod() {
        return method;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((method == null) ? 0 : method.hashCode());
        result = prime * result + ((signature == null) ? 0 : signature.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RpcMeta other = (RpcMeta) obj;
        if (method == null) {
            if (other.method != null) {
                return false;
            }
        } else if (!method.equals(other.method)) {
            return false;
        }
        if (signature == null) {
            if (other.signature != null) {
                return false;
            }
        } else if (!signature.equals(other.signature)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "RpcMeta [signature=" + signature + ", method=" + method + "]";
    }
}
