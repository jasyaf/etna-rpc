package org.etnaframework.rpc.client;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.etnaframework.core.logging.Log;
import org.etnaframework.core.spring.SpringContext;
import org.etnaframework.core.spring.annotation.Config;
import org.etnaframework.core.util.DatetimeUtils;
import org.etnaframework.core.util.DatetimeUtils.Datetime;
import org.etnaframework.core.util.HumanReadableUtils;
import org.etnaframework.core.util.StringTools;
import org.etnaframework.core.util.SystemInfo;
import org.etnaframework.core.util.ThreadUtils.NamedThreadFactory;
import org.etnaframework.rpc.codec.RpcCodecFactory;
import org.etnaframework.rpc.codec.RpcRequest;
import org.etnaframework.rpc.codec.RpcResponse;
import org.etnaframework.rpc.exception.RpcClientCaughtException;
import org.etnaframework.rpc.exception.RpcClientWaitTimeoutException;
import org.etnaframework.rpc.exception.RpcConnectToServerException;
import org.etnaframework.rpc.exception.RpcServerInvocationException;
import org.slf4j.Logger;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * RPC调用的客户端，每个远程服务器host:port都将对应一个客户端实例
 *
 * @author BlackCat
 * @since 2015-04-10
 */
@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class RpcClient extends SimpleChannelInboundHandler<RpcResponse> {

    /** 远程服务器host:port字符串对对应的请求客户端的映射 */
    private static Map<String, RpcClient> clients = new ConcurrentHashMap<String, RpcClient>();

    /** 消息ID生成器，服务器返回时会返回相同的ID，用于客户端识别是哪个请求的回包并进行接下来的处理 */
    private static AtomicLong idGen = new AtomicLong();

    protected final Logger log = Log.getLogger();

    @Config(value = "etna.rpc.client.tcpNoDelay", resetable = false)
    private boolean tcpNoDelay = true;

    @Config(value = "etna.rpc.client.soKeepAlive", resetable = false)
    private boolean soKeepAlive = true;

    /** 连接超时毫秒数，目前是全局设置的，后续考虑可针对连接的服务器单独设置 */
    @Config(value = "etna.rpc.client.connectTimeoutMillis", resetable = false)
    private int connectTimeoutMillis = Datetime.MILLIS_PER_SECOND * 2;

    @Config(value = "etna.rpc.client.soRcvBuf", resetable = false)
    private int soRcvBuf = 8192;

    /**
     * <pre>
     * 决定当对Socket执行close方法时的执行策略 <0 执行close方法后不会立即关闭网络连接，它会延迟一段时间，等完剩余的数据发送完毕，才会真正关闭释放资源 =0 close后立即关闭并释放资源 >0 close方法会最多阻塞X秒，在X秒内如果数据发送完毕，即立即释放资源返回；如果超过X秒数据还没发完，则不再理会，立即关闭资源返回 </pre>
     */
    @Config(value = "etna.rpc.client.soLinger", resetable = false)
    private int soLinger = 0;

    @Config(value = "etna.rpc.client.workerCount", resetable = false)
    private int workerCount = SystemInfo.CORE_PROCESSOR_NUM * 2;

    /** 远程服务器的host */
    private String _host;

    /** 远程服务器的端口号 */
    private int _port;

    private Bootstrap bootstrap;

    /** 与远程服务器保持的单连接，全双工模式，能同时发送请求/接收回包 */
    private Channel channel;

    /** RPC调用时，本地客户端最多等待结果的时间，单位毫秒 */
    @Config("etna.rpc.client.maxWaitForMs")
    private int maxWaitForMs = Datetime.MILLIS_PER_SECOND * 15;

    /** 记录发往远程服务器的sequence和对应的请求/回包数据，远程服务器回包的顺序不一定按发包顺序回，必须通过sequence来做标识 */
    private Map<Long, RpcEvent> requestMap = new ConcurrentHashMap<Long, RpcEvent>();

    private RpcClient() {
    }

    /**
     * 通过host:port获取RPC客户端的实例，本地如果没有的话就初始化一个新的
     */
    public static RpcClient getInstance(String host, int port) {
        String key = host + ":" + port;
        RpcClient client = clients.get(key);
        if (null == client) {
            synchronized (clients) {
                client = clients.get(key);
                if (null == client) {
                    client = SpringContext.getBean(RpcClient.class);
                    client.init(host, port);
                    clients.put(key, client);
                }
            }
        }
        return client;
    }

    private void init(String host, int port) {
        this._host = host;
        this._port = port;

        bootstrap = new Bootstrap();
        // 线程池的名称使用远程服务器的host:port
        bootstrap.group(new NioEventLoopGroup(workerCount, new NamedThreadFactory("NioWorker-" + "RPC-Cli(->" + host + ":" + port + ")-", Thread.MAX_PRIORITY)));
        bootstrap.channel(NioSocketChannel.class);

        bootstrap.option(ChannelOption.TCP_NODELAY, tcpNoDelay);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, soKeepAlive);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis);
        bootstrap.option(ChannelOption.SO_RCVBUF, soRcvBuf);
        bootstrap.option(ChannelOption.SO_LINGER, soLinger);

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                p.addLast("decoder", RpcCodecFactory.createDecoder(RpcClient.this));
                p.addLast("encoder", RpcCodecFactory.createEncoder(RpcClient.this));
                p.addLast("hander", RpcClient.this);
            }
        });

        // 初始化完成后就尝试建立连接，只跟远程服务器保持一个连接，全双工模式，如果连接失败直接可报错
        ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(_host, _port));
        this.channel = connectFuture.awaitUninterruptibly().channel();
        if (channel.isActive()) {
            log.info("connect to RPC Server [" + _host + "(" + _port + ")] OK");
        } else {
            throw new RpcConnectToServerException("connect to RPC Server [" + host + ":" + port + "] FAILED");
        }
    }

    /**
     * <pre>
     * 将RPC请求发送到远程服务器
     *
     * 同步模式下将挂起当前线程等待回包唤醒或超时唤醒
     * 异步模式下将在发送后继续接下来的业务逻辑，当有回包或超时时由系统调用callback方法
     * </pre>
     */
    RpcResponse send(RpcRequest req) throws Throwable {
        req.sequence = idGen.incrementAndGet();
        RpcEvent re = new RpcEvent(req);
        requestMap.put(req.sequence, re);
        try {
            if (!channel.isActive()) { // 发送前需要检查一下连接是否有效
                throw new RpcConnectToServerException("connection to RPC Server [" + _host + ":" + _port + "] is INACTIVE");
            }
            channel.write(req);
            channel.flush();
            synchronized (re) {
                try {
                    re.wait(maxWaitForMs);
                    // 执行下列代码时，要么是服务器已经回包了，将线程唤醒，要么是超时唤醒
                    // 服务器回包的话response必然有东西，不会是null
                    RpcResponse resp = re.response;
                    if (null != resp) {
                        if (null != resp.error) { // 判断服务器端是否抛出了异常，有异常要在客户端再次抛出
                            throw new RpcServerInvocationException("\n\n" //
                                + "************************ RPC Remote Server Exception ***************************" + "\n" + re.getRequestInfo() + "\n" + resp.error + "\n" + "************************ RPC Remote Server Exception ***************************" + "\n");
                        }
                        return resp;
                    }
                    // 判断是否是异常导致的唤醒，没有异常就表明是超时唤醒
                    if (null != re.cause) {
                        throw new RpcClientCaughtException("\n\n" //
                            + "************************ RPC Client Caught Exception ***************************" + "\n" + re.getRequestInfo() + "\n" + re.cause + "\n" + "************************ RPC Client Caught Exception ***************************" + "\n");
                    }
                    if (re.disconnected) { // 连接丢失
                        throw new RpcConnectToServerException("connection to RPC Server [" + _host + ":" + _port + "] is LOST");
                    }
                    throw new RpcClientWaitTimeoutException("\n\n" //
                        + "************************ RPC Client Timeout Exception **************************" + "\n" + re.getRequestInfo() + "************************ RPC Client Timeout Exception **************************" + "\n");
                } catch (InterruptedException e) {
                    log.error("wait RPC response INTERRUPTED, req:{}", req);
                    throw e;
                }
            }
        } finally {
            requestMap.remove(req.sequence);
        }
    }

    /**
     * <pre>
     * 收到远程服务器的回包
     *
     * 同步模式下唤醒之前挂起的线程，并执行后续业务代码
     * 异步模式下执行对应的callback方法
     * </pre>
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse resp) throws Exception {
        log.debug("RPC response: {}", resp);
        RpcEvent re = requestMap.get(resp.sequence);
        if (null == re) { // 找不到对应的请求信息，说明请求已经不需要处理了，丢包即可
            log.warn("sequence " + resp.sequence + " not found int requestMap");
            return;
        }
        re.response = resp;
        synchronized (re) {
            re.notifyAll();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("RPC channelActive: {}", ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("RPC channelInactive: {}", ctx.channel());
        String key = _host + ":" + _port;
        synchronized (clients) {
            clients.remove(key); // 客户端被移除，下次请求时会重新建立连接
        }
        // 所有等待回包处理的请求，全部按连接失败处理
        if (!requestMap.isEmpty()) {
            for (RpcEvent re : requestMap.values()) {
                re.disconnected = true;
                synchronized (re) {
                    re.notifyAll();
                }
            }
        }
    }

    /**
     * 一旦出现不能自动处理的异常，就必须把连接断掉，所有等待回包处理的请求按请求异常处理
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        try {
            if (!requestMap.isEmpty()) {
                String c = StringTools.printThrowable(cause) + Thread.currentThread().getName();
                for (RpcEvent re : requestMap.values()) {
                    re.cause = c;
                    synchronized (re) {
                        re.notifyAll();
                    }
                }
            }
        } finally {
            ctx.close();
        }
    }

    /**
     * RPC请求/回包事件
     */
    public class RpcEvent {

        public RpcRequest request;

        public volatile RpcResponse response;

        /** 请求发送时间，用于客户端判断是否超时 */
        public long sendTime = System.currentTimeMillis();

        /** 如果在连接中遇到了未获处理的异常，需要保存下来预备后续报告 */
        public String cause;

        /** 是否在等待回包时连接断开（连接丢失），当为true时应当抛出{@link RpcConnectToServerException} */
        public boolean disconnected;

        public RpcEvent(RpcRequest request) {
            this.request = request;
        }

        /**
         * 报告异常时，输出本次请求的信息
         */
        public String getRequestInfo() {
            return "RPC服务器地址    " + _host + ":" + _port + "\n" + "RPC请求发送时间  " + DatetimeUtils.format(sendTime) + "\n" + "等待返回超时设置 " + HumanReadableUtils.timeSpan(
                maxWaitForMs) + "\n" + "客户端已等待时间 " + HumanReadableUtils.timeSpan(System.currentTimeMillis() - sendTime) + "\n" + "RPC请求内容      " + request + "\n";
        }
    }
}
