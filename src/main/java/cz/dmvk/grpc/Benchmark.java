package cz.dmvk.grpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import cz.dmvk.grpc.proto.BenchmarkGrpc;
import cz.dmvk.grpc.proto.Request;
import cz.dmvk.grpc.proto.Response;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Cork;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Benchmark {

  private static final boolean CORKING_ENABLED = true;
  private static final int PORT = 1337;
  private static final int REQUEST_PER_CORK = 1000;
  private static final int MAX_INFLIGHT = 100_000;

  public static void main(String[] args) throws InterruptedException, IOException {

    final Server server = ServerBuilder.forPort(PORT)
        .intercept(new ServerCorkingInterceptor())
        .addService(new BenchmarkService())
        .build()
        .start();

    final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", PORT)
        .usePlaintext()
        .intercept(new ClientCorkingInterceptor())
        .build();

    final CountDownLatch completed = new CountDownLatch(1);
    // max pending
    final Semaphore semaphore = new Semaphore(MAX_INFLIGHT);

    final StreamObserver<Request> requestStream = BenchmarkGrpc.newStub(channel)
        .run(new StreamObserver<Response>() {

          @Override
          public void onNext(Response value) {
            semaphore.release();
          }

          @Override
          public void onError(Throwable t) {

          }

          @Override
          public void onCompleted() {
            completed.countDown();
          }
        });

    final Thread requestThread = new Thread(() -> {
      try {
        final Request request = Request.getDefaultInstance();
        while (!Thread.interrupted()) {
          semaphore.acquire();
          requestStream.onNext(request);
        }
      } catch (InterruptedException e) {
        // noop
      } finally {
        requestStream.onCompleted();
      }
    });

    requestThread.setName("consumer");
    requestThread.setDaemon(true);
    requestThread.start();

    final CountDownLatch shutdown = new CountDownLatch(1);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        requestThread.interrupt();
        requestThread.join();
        completed.await();
        channel.shutdownNow();
        server.shutdownNow();
        shutdown.countDown();
      } catch (InterruptedException e) {
        // noop
      }
    }));

    shutdown.await();
  }

  private static class ClientCorkingInterceptor implements ClientInterceptor {

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      if (!CORKING_ENABLED) {
        return next.newCall(method, callOptions);
      }
      return new SimpleForwardingClientCall<ReqT, RespT>(
          next.newCall(method, callOptions)) {

        Cork currentCork;
        int count = 0;

        @Override
        public void sendMessage(ReqT message) {
          if (currentCork == null) {
            currentCork = cork();
          }
          if (count++ % REQUEST_PER_CORK == 0) {
            currentCork.close();
            currentCork = null;
          }
          super.sendMessage(message);
        }

        @Override
        public void halfClose() {
          if (currentCork != null) {
            currentCork.close();
            currentCork = null;
          }
          super.halfClose();
        }
      };
    }
  }

  private static class ServerCorkingInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call,
        Metadata metadata,
        ServerCallHandler<ReqT, RespT> next) {
      if (!CORKING_ENABLED) {
        return next.startCall(call, metadata);
      }
      return next.startCall(new SimpleForwardingServerCall<ReqT, RespT>(call) {

        Cork currentCork;
        int count = 0;

        @Override
        public void sendMessage(RespT message) {
          if (currentCork == null) {
            currentCork = cork();
          }
          if (count++ % REQUEST_PER_CORK == 0) {
            currentCork.close();
            currentCork = null;
          }
          super.sendMessage(message);
        }

        @Override
        public void close(Status status, Metadata trailers) {
          if (currentCork != null) {
            currentCork.close();
            currentCork = null;
          }
          super.close(status, trailers);
        }
      }, metadata);
    }
  }

  private static class BenchmarkService extends BenchmarkGrpc.BenchmarkImplBase {

    private final ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("scheduler-%d")
                .build());

    private final AtomicLong numRequests = new AtomicLong();
    private final AtomicLong previousNumRequests = new AtomicLong();

    @Override
    public StreamObserver<Request> run(StreamObserver<Response> responseObserver) {

      scheduler.scheduleAtFixedRate(() -> {
        final long current = numRequests.get();
        final long previous = previousNumRequests.getAndSet(current);
        System.out.println("Total: " + current);
        System.out.println("Diff: " + (current - previous));
      }, 1000, 1000, TimeUnit.MILLISECONDS);

      final long startTime = System.currentTimeMillis();
      final Response response = Response.newBuilder().build();
      return new StreamObserver<Request>() {

        @Override
        public void onNext(Request value) {
          numRequests.incrementAndGet();
          responseObserver.onNext(response);
        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onCompleted() {
          final long endTime = System.currentTimeMillis();
          System.out.println("Took: " + (endTime - startTime));
          responseObserver.onCompleted();
        }
      };
    }
  }
}
