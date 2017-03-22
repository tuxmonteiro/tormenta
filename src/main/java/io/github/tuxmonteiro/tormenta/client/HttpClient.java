package io.github.tuxmonteiro.tormenta.client;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.HostStats;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static org.asynchttpclient.AsyncHttpClientConfig.ResponseBodyPartFactory.*;
import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

@Service
public class HttpClient {

    private static final AtomicLong MAX_CONN_USED = new AtomicLong(0);
    private static final AtomicLong LAST_TIME_STAMP_TEST = new AtomicLong(0);
    private static final AtomicLong REQUEST_TIME_MAX = new AtomicLong(0);
    private static final Map<Integer, Long> STATUS_MAP = new ConcurrentHashMap<>();
    private static final AtomicLong BODY_TOTAL_SIZE = new AtomicLong(0);
    private static final AtomicLong TOTAL_CONNECTION_COUNT = new AtomicLong(0);
    private static final AtomicReference<String> LAST_LOG_MESSAGE = new AtomicReference<>("");
    private static final AtomicInteger STATUS_OK_COUNT = new AtomicInteger(0);

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final int numInter = 500000;
    private final int hostLimit = 1;
    private final AsyncHttpClient[] asyncHttpClients = new AsyncHttpClient[hostLimit];
    private final List<Future<Boolean>> allHasFinished = new ArrayList<>();
    private final int pooledConnectionIdleTimeout = 1000;
    private final int maxConnectionsPerHost = 10000;
    private final int connectTimeout = 10000;

    @PostConstruct
    private void run() throws IOException, InterruptedException {
        final long allStart = System.currentTimeMillis();

        IntStream.rangeClosed(0, hostLimit - 1).forEach(id ->
                asyncHttpClients[id] = asyncHttpClient(
                        config()
                            .setKeepAlive(true)
                            .setConnectTimeout(connectTimeout)
                            .setPooledConnectionIdleTimeout(pooledConnectionIdleTimeout)
                            .setMaxConnectionsPerHost(maxConnectionsPerHost)
                            .setResponseBodyPartFactory(EAGER).build())
        );

        IntStream.rangeClosed(1, numInter).parallel().forEach(iter -> {
            int hostId = iter % hostLimit;
            final String hostWithPath = "http://127.0.0." + (hostId + 1) + ":8080/1m";

            TOTAL_CONNECTION_COUNT.set(asyncHttpClients[hostId].getClientStats().getTotalConnectionCount());
            if (TOTAL_CONNECTION_COUNT.get() > MAX_CONN_USED.get()) MAX_CONN_USED.set(TOTAL_CONNECTION_COUNT.get());
            while (TOTAL_CONNECTION_COUNT.get() >= (maxConnectionsPerHost - (maxConnectionsPerHost * 0.1))) {
                logger.warn("overrun (" + TOTAL_CONNECTION_COUNT + ") connections");
                TOTAL_CONNECTION_COUNT.set(asyncHttpClients[hostId].getClientStats().getTotalConnectionCount());
            }

            final Future<Boolean> hasFinished = asyncHttpClients[hostId]
                    .prepareGet(hostWithPath)
                    .execute(new AsyncHandler<Boolean>() {
                        volatile long bodyLength = -1;
                        final long start = System.currentTimeMillis();

                        @Override
                        public void onThrowable(Throwable throwable) {
                            STATUS_MAP.compute(500, (k, v) -> v == null ? 1 : v + 1);
                            if (!throwable.getMessage().equals(LAST_LOG_MESSAGE.get())) {
                                logger.error(throwable.getMessage());
                                LAST_LOG_MESSAGE.set(throwable.getMessage());
                            }
                        }

                        @Override
                        public State onBodyPartReceived(HttpResponseBodyPart httpResponseBodyPart) throws Exception {
                            if (httpResponseBodyPart.isLast() && bodyLength == -1) {
                                bodyLength = httpResponseBodyPart.length();
                            }
                            return State.CONTINUE;
                        }

                        @Override
                        public State onStatusReceived(HttpResponseStatus httpResponseStatus) throws Exception {
                            final int statusCode = httpResponseStatus.getStatusCode();
                            if (statusCode >= 100 && statusCode <= 399) {
                                STATUS_OK_COUNT.incrementAndGet();
                            }
                            STATUS_MAP.compute(statusCode, (k, v) -> v == null ? 1 : v + 1);
                            return State.CONTINUE;
                        }

                        @Override
                        public State onHeadersReceived(HttpResponseHeaders httpResponseHeaders) throws Exception {
                            if (httpResponseHeaders.getHeaders().contains(CONTENT_LENGTH)) {
                                bodyLength = httpResponseHeaders.getHeaders().getInt(CONTENT_LENGTH);
                                long requestTime = System.currentTimeMillis() - start;
                                if (requestTime > REQUEST_TIME_MAX.get()) REQUEST_TIME_MAX.set(requestTime);
                            }
                            return State.CONTINUE;
                        }

                        @Override
                        public Boolean onCompleted() throws Exception {
                            BODY_TOTAL_SIZE.addAndGet(bodyLength);
                            long requestTime = System.currentTimeMillis() - start;
                            if (requestTime > REQUEST_TIME_MAX.get()) REQUEST_TIME_MAX.set(requestTime);
                            if (System.currentTimeMillis() > LAST_TIME_STAMP_TEST.get()) LAST_TIME_STAMP_TEST.set(System.currentTimeMillis());
                            return true;
                        }
                    });
            allHasFinished.add(hasFinished);
        });

        long timelimit = 60000;
        long loopStart = System.currentTimeMillis();
        while (allHasFinished.stream().map(f -> {
            try {
                return f != null ? f.get() || f.isDone() || f.isCancelled() : Boolean.TRUE;
            } catch (InterruptedException | ExecutionException e) {
                logger.error(e.getMessage());
                return Boolean.FALSE;
            }
        }).filter(b -> !b).count() > 0) {
            if (System.currentTimeMillis() > loopStart + timelimit) {
                break;
            }
        }
        allHasFinished.clear();

        long testTime = LAST_TIME_STAMP_TEST.get() - allStart;
        long testTimeSec = TimeUnit.MILLISECONDS.toSeconds(testTime);

        long numConn;
        Set<Map.Entry<String, HostStats>> clientsStats;
        do {
            Stream<Map.Entry<String, HostStats>> streamOfClientsStats = Arrays.stream(asyncHttpClients).map(AsyncHttpClient::getClientStats).flatMap(s -> s.getStatsPerHost().entrySet().stream());
            clientsStats = streamOfClientsStats.collect(Collectors.toSet());
            numConn = clientsStats.stream().reduce(0L, (sum, e) -> sum += e.getValue().getHostConnectionCount(), (sum1, sum2) -> sum1 + sum2);
        } while (numConn > hostLimit || !clientsStats.isEmpty());

        if (clientsStats.isEmpty()) {
            logger.info("[ClientStats] no stats");
        } else {
            int numClients = clientsStats.size();
            logger.info("[ClientStats] num clients: " + numClients + " , num conns: " + numConn);
        }

        logger.info("request time [avg ms]: " + (double)testTime/(double)STATUS_OK_COUNT.get());
        logger.info("request time [max ms]: " + REQUEST_TIME_MAX.get());
        logger.info("body total [bytes]: " + BODY_TOTAL_SIZE.get());
        STATUS_MAP.entrySet().forEach(e -> logger.info("total " + e.getKey() + ": " + e.getValue()));
        STATUS_MAP.clear();
        logger.info("request/sec [avg]: " + (testTimeSec != 0L ? (STATUS_OK_COUNT.get() / testTimeSec) : "INF"));
        logger.info("maxconn used: " + MAX_CONN_USED.get() + " connections");
        logger.info("total test time: " + testTimeSec + " seconds");

    }
}
