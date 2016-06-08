package com.github.trask;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.github.trask.DownstreamServiceGrpc;
import org.github.trask.DownstreamServiceGrpc.DownstreamService;
import org.github.trask.Hello.ClientReply;
import org.github.trask.Hello.ServerRequest;
import org.junit.Test;

public class ConcurrentServerPushTest {

    @Test
    public void test() throws Exception {
        final DownstreamServiceImpl downstreamService = new DownstreamServiceImpl();
        Server server = NettyServerBuilder.forPort(8025)
                .addService(DownstreamServiceGrpc.bindService(downstreamService))
                .build()
                .start();

        ManagedChannel channel = NettyChannelBuilder
                .forAddress("localhost", 8025)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();

        final int total = 100;

        CountingStreamObserver<ServerRequest> countingStreamObserver =
                new CountingStreamObserver<ServerRequest>();
        DownstreamService downstreamClient = DownstreamServiceGrpc.newStub(channel);
        StreamObserver<ClientReply> clientReplyObserver =
                downstreamClient.connect(countingStreamObserver);
        countingStreamObserver.clientReplyObserver = clientReplyObserver;

        while (downstreamService.responseObserver == null) {
            Thread.sleep(10);
        }

        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < total; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    sendServerRequest(downstreamService);
                }
            });
        }

        while (countingStreamObserver.count.get() < total) {
            Thread.sleep(1000);
            System.out.println("received " + countingStreamObserver.count.get() + " messages");
        }

        channel.shutdown();
        server.shutdown();
    }

    private static void sendServerRequest(DownstreamServiceImpl server) {
        server.responseObserver.onNext(ServerRequest.getDefaultInstance());
    }

    private static class DownstreamServiceImpl implements DownstreamService {

        private volatile StreamObserver<ServerRequest> responseObserver;

        @Override
        public StreamObserver<ClientReply> connect(StreamObserver<ServerRequest> responseObserver) {
            this.responseObserver = responseObserver;
            return new NopStreamObserver<ClientReply>();
        }
    }

    private static class CountingStreamObserver<T> implements StreamObserver<T> {

        private final AtomicInteger count = new AtomicInteger();

        private volatile StreamObserver<ClientReply> clientReplyObserver;

        @Override
        public void onNext(T value) {
            count.getAndIncrement();
            clientReplyObserver.onNext(ClientReply.getDefaultInstance());
        }

        @Override
        public void onError(Throwable t) {
            t.printStackTrace();
            count.getAndIncrement();
        }

        @Override
        public void onCompleted() {
            count.getAndIncrement();
        }
    }

    private static class NopStreamObserver<T> implements StreamObserver<T> {

        @Override
        public void onNext(T value) {}

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {}
    }
}
