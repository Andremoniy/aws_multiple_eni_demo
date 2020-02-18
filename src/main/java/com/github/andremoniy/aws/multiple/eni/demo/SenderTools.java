package com.github.andremoniy.aws.multiple.eni.demo;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public enum SenderTools {
    ;

    public static final int BLOCK_SIZE = 8500;
    public static final int PORT = 8080;

    public static List<NetworkInterface> getNetworkInterfaces() throws SocketException {
        return NetworkInterface
                    .networkInterfaces()
                    .filter(networkInterface -> !networkInterface.isVirtual())
                    .filter(networkInterface -> networkInterface.getName().startsWith("e"))
                    .collect(Collectors.toUnmodifiableList());
    }

    public static int getLastChunkSize(long size, int lastChunkNumber) {
        return (int) (size - BLOCK_SIZE * (lastChunkNumber - 1));
    }

    public static int getLastChunkNumber(double size) {
        return (int) Math.ceil(size / BLOCK_SIZE);
    }

    public static Optional<InetAddress> getInet4Address(NetworkInterface networkInterface) {
        return networkInterface
                        .inetAddresses()
                        .filter(inetAddress -> inetAddress instanceof Inet4Address)
                        .findFirst();
    }
}
