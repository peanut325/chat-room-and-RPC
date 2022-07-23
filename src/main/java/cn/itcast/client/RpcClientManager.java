package cn.itcast.client;

import cn.itcast.client.handler.RpcResponseMessageHandler;
import cn.itcast.message.RpcRequestMessage;
import cn.itcast.protocol.MessageCodecSharable;
import cn.itcast.protocol.ProtocolFrameDecoder;
import cn.itcast.protocol.SequenceIdGenerator;
import cn.itcast.server.service.HelloService;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultPromise;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Proxy;

@Slf4j
public class RpcClientManager {

    private static Channel channel = null;
    private static final Object LOCK = new Object();

    public static void main(String[] args) {
        HelloService proxyService = getProxyService(HelloService.class);
        System.out.println(proxyService.sayHello("zhangsan"));
        System.out.println(proxyService.sayHello("lisi"));
    }

    // 获取代理对象
    public static <T> T getProxyService(Class<T> serviceClass) {
        ClassLoader classLoader = serviceClass.getClassLoader();
        Class<?>[] interfaces = new Class[]{HelloService.class};

        Object proxyInstance = Proxy.newProxyInstance(classLoader,
                interfaces,
                (proxy, method, args) -> {
                    // 1. 将方法调用转换为 消息对象
                    int sequenceId = SequenceIdGenerator.nextId();

                    RpcRequestMessage rpcRequestMessage = new RpcRequestMessage(
                            sequenceId,
                            serviceClass.getName(),
                            method.getName(),
                            method.getReturnType(),
                            method.getParameterTypes(),
                            args
                    );

                    // 2. 将消息对象发送出去
                    getChannel().writeAndFlush(rpcRequestMessage);

                    // 3. 准备一个空 Promise 对象，来接收结果             指定 promise 对象异步接收结果线程
                    DefaultPromise<Object> promise = new DefaultPromise<>(getChannel().eventLoop());
                    // 设置进map
                    RpcResponseMessageHandler.PROMISES.put(sequenceId, promise);
                    // 4. 等待 promise 结果
                    // 阻塞等待responseHandler唤醒
                    promise.await();
                    if (promise.isSuccess()) {
                        return promise.getNow();
                    } else {
                        // 有问题抛异常
                        throw new RuntimeException(promise.cause());
                    }

                });

        return (T) proxyInstance;
    }

    // 单例模式
    public static Channel getChannel() {
        if (channel != null) {
            return channel;
        }

        synchronized (LOCK) {
            if (channel != null) {
                return channel;
            }
            initChannel();
            return channel;
        }
    }

    public static void initChannel() {
        NioEventLoopGroup group = new NioEventLoopGroup();
        LoggingHandler LOGGING_HANDLER = new LoggingHandler(LogLevel.DEBUG);
        MessageCodecSharable MESSAGE_CODEC = new MessageCodecSharable();

        // rpc 响应消息处理器，待实现
        RpcResponseMessageHandler RPC_HANDLER = new RpcResponseMessageHandler();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.group(group);
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new ProtocolFrameDecoder());
                    ch.pipeline().addLast(LOGGING_HANDLER);
                    ch.pipeline().addLast(MESSAGE_CODEC);
                    ch.pipeline().addLast(RPC_HANDLER);
                }
            });
            channel = bootstrap.connect("localhost", 8081).sync().channel();

            ChannelFuture future = channel.writeAndFlush(new RpcRequestMessage(
                    1,
                    "cn.itcast.server.service.HelloService",
                    "sayHello",
                    String.class,
                    new Class[]{String.class},
                    new Object[]{"张三"}
            )).addListener(promise -> {
                if (!promise.isSuccess()) {
                    Throwable cause = promise.cause();
                    log.error("error", cause);
                }
            });

            // 异步关闭
            channel.closeFuture().addListener(future1 -> {
                group.shutdownGracefully();
            });
        } catch (Exception e) {
            log.error("client error", e);
        }
    }

}
