package com.github.andremoniy.aws.multiple.eni.demo.daemon;

import com.github.andremoniy.aws.multiple.eni.demo.util.SenderTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 *
 */
public class SenderDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(SenderDaemon.class);

    public static void main(String[] args) throws IOException {
        final boolean fakeWritings = args.length > 0 && "fake".equals(args[0]);

        if (fakeWritings) {
            LOGGER.warn("The mode of fake writing has been enabled (no actual writing on disk will be performed)");
        }

        final String preferedNetworkInterfaceName = args.length > 1 ? args[1] : null;

        final List<NetworkInterface> networkInterfaces;
        if (preferedNetworkInterfaceName != null) {
            networkInterfaces = SenderTools.getNetworkInterfaces()
                    .stream()
                    .filter(networkInterface -> preferedNetworkInterfaceName.equals(networkInterface.getName()))
                    .collect(Collectors.toUnmodifiableList());
        } else {
            networkInterfaces = SenderTools.getNetworkInterfaces();
        }
        LOGGER.info("Found {} network interfaces", networkInterfaces.size());

        final var executorService = Executors.newFixedThreadPool(networkInterfaces.size() + 1);
        final TransactionsManager transactionsManager = new TransactionsManager(fakeWritings);
        executorService.submit(transactionsManager);

        final List<Future<?>> futures = new ArrayList<>();

        for (var networkInterface : networkInterfaces) {
            final Optional<InetAddress> firstInet4Address = SenderTools.getInet4Address(networkInterface);

            if (firstInet4Address.isEmpty()) {
                LOGGER.info("There is no Inet4 address in the interface {}, skipping", networkInterface);
                continue;
            }

            final Future<?> future = executorService.submit(new SocketListener(transactionsManager, firstInet4Address.get()));

            futures.add(future);
        }

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOGGER.error(e.getMessage(), e);
            }
        });
    }

}
