package org.etnaframework.rpc.server;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.etnaframework.core.logging.Log;
import org.etnaframework.core.spring.BootstrapModule;
import org.etnaframework.core.spring.annotation.Config;
import org.etnaframework.core.spring.annotation.OnContextInited;
import org.etnaframework.core.util.SystemInfo;
import org.etnaframework.core.util.ThreadUtils.NamedThreadFactory;
import org.etnaframework.rpc.codec.RpcCodecFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * <pre>
 * RPC服务的启动类，请在业务代码中继承该类并在继承类上加@{@link Service}注解
 *
 * 本类实现了{@link BootstrapModule}接口，etna框架在启动时，当http服务启动后
 * 会扫描包路径下所有的{@link BootstrapModule}实现
 * 并调用{@link #init()} -> {@link #bind()}来完成初始化启动并绑定端口
 *
 * 请在实现类的{@link #configure()}方法中设置将要使用的端口
 * </pre>
 *
 * @author BlackCat
 * @since 2015-04-10
 */
public abstract class RpcServer implements BootstrapModule {

    protected final Logger log = Log.getLogger();

    @Config(value = "etna.rpc.server.tcpNoDelay", resetable = false)
    protected boolean tcpNoDelay = true;

    @Config(value = "etna.rpc.server.soRcvBuf", resetable = false)
    protected int soRcvBuf = 8192;

    /** RPC服务预备绑定的端口 */
    protected List<InetSocketAddress> ports = new ArrayList<InetSocketAddress>();

    @Config(value = "etna.rpc.server.bossCount", resetable = false)
    private int bossCount = 1;

    @Config(value = "etna.rpc.server.workerCount", resetable = false)
    private int workerCount = SystemInfo.CORE_PROCESSOR_NUM * 2;

    @Autowired
    private RpcServerHandler rpcServerHandler;

    private ServerBootstrap bootstrap;

    @Override
    public final List<InetSocketAddress> getPorts() {
        return ports;
    }

    @Override
    public final void bind() throws Throwable {
        for (InetSocketAddress isa : ports) {
            bootstrap.bind(isa).await();
        }
    }

    /**
     * 初始化RPC服务，准备netty配置，准备绑定端口
     */
    @OnContextInited
    protected final void init() throws Throwable {
        // 线程池的名称采用模块的类名
        EventLoopGroup bossGroup = new NioEventLoopGroup(bossCount, new NamedThreadFactory("NioBoss-" + getClass().getSimpleName() + "-", Thread.MAX_PRIORITY));
        EventLoopGroup workerGroup = new NioEventLoopGroup(workerCount, new NamedThreadFactory("NioWorker-" + getClass().getSimpleName() + "-", Thread.MAX_PRIORITY));

        bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup);
        bootstrap.channel(NioServerSocketChannel.class);

        bootstrap.childOption(ChannelOption.TCP_NODELAY, tcpNoDelay);
        bootstrap.childOption(ChannelOption.SO_RCVBUF, soRcvBuf);

        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                p.addLast("decoder", RpcCodecFactory.createDecoder(rpcServerHandler));
                p.addLast("encoder", RpcCodecFactory.createEncoder(rpcServerHandler));
                p.addLast("handler", rpcServerHandler);
            }
        });

        configure();

        if (ports.isEmpty()) { // 检查是否设置了绑定端口
            throw new IllegalArgumentException(getClass().getName() + "模块没有指定绑定端口，请在configure方法中添加");
        }
    }

    /**
     * 设置RPC服务所需绑定的端口，进行初始化操作
     */
    protected abstract void configure() throws Throwable;
}
