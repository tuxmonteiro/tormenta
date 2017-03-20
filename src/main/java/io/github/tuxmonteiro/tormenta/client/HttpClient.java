package io.github.tuxmonteiro.tormenta.client;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.HostStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.asynchttpclient.AsyncHttpClientConfig.ResponseBodyPartFactory.*;
import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

@Service
public class HttpClient {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @PostConstruct
    private void run() throws IOException, InterruptedException {
        final long allStart = System.currentTimeMillis();
        int numInter = 50000;
        int hostLimit = 254;
        final AtomicLong requestTimeMax = new AtomicLong(0);
        final Map<Integer, Long> statusMap = new ConcurrentHashMap<>();
        final AtomicLong bodyTotalSize = new AtomicLong(0);
        final AsyncHttpClient[] asyncHttpClients = new AsyncHttpClient[hostLimit];
        IntStream.rangeClosed(0, hostLimit - 1).forEach(id -> asyncHttpClients[id] = asyncHttpClient(config().setKeepAlive(true).setMaxConnectionsPerHost(10).setResponseBodyPartFactory(EAGER).build()));

        IntStream.rangeClosed(1,numInter).forEach(iter -> {
            final long start = System.currentTimeMillis();
            int hostId = iter % hostLimit;
            final String hostWithPath = "http://127.0.0." + (hostId + 1) + ":8080/";
            asyncHttpClients[hostId]
                    .prepareGet(hostWithPath)
                    .execute()
                    .toCompletableFuture()
                    .thenAccept(response -> {
                        final int statusCode = response.getStatusCode();
                        statusMap.compute(statusCode, (k, v) -> v == null ? 1 : v + 1);
                        logger.debug("status: " + statusCode);
                        long bodySize = response.getResponseBody().length();
                        bodyTotalSize.addAndGet(bodySize);
                        logger.debug("bytes: " + bodySize);
                        long requestTime = System.currentTimeMillis() - start;
                        logger.debug("request time: " + requestTime);
                        if (requestTime > requestTimeMax.get()) requestTimeMax.set(requestTime);
                    })
                    .join();
        });
        long testTime = System.currentTimeMillis() - allStart;

        Stream<Map.Entry<String, HostStats>> streamOfClientsStats = Arrays.stream(asyncHttpClients).map(AsyncHttpClient::getClientStats).flatMap(s -> s.getStatsPerHost().entrySet().stream());
        final Set<Map.Entry<String, HostStats>> clientsStats = streamOfClientsStats.collect(Collectors.toSet());
        int numClients = clientsStats.size();
        long numConn = clientsStats.stream().reduce(0L, (sum, e) -> sum += e.getValue().getHostConnectionCount(), (sum1, sum2) -> sum1 + sum2);
        if (clientsStats.isEmpty()) {
            logger.info("[ClientStats] no stats");
        } else {
            logger.info("[ClientStats] num clients: " + numClients + " , num conns: " + numConn);
        }
        logger.info("request time [avg]: " + testTime/numInter);
        logger.info("request time [max]: " + requestTimeMax.get());
        logger.info("body total [bytes]: " + bodyTotalSize.get());
        statusMap.entrySet().forEach(e -> logger.info("total " + e.getKey() + ": " + e.getValue()));
        logger.info("request/sec [avg]: " + numInter/TimeUnit.MILLISECONDS.toSeconds(testTime));
        logger.info("total test time: " + TimeUnit.MILLISECONDS.toSeconds(testTime) + " seconds");
    }
}
