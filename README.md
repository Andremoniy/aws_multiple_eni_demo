# aws_multiple_eni_demo
A demo testing utilisation of multiple ENIs to transfer and receive data

### With actual disk writing:
```
1 ENI:

1)
2020-02-18 13:24:01 INFO  [main] SenderClient:121 - Uploading time: 14388222071 ns, 14 s
2020-02-18 13:24:01 INFO  [main] SenderClient:124 - Throughput: 569.3545706742518 MBs, 0.556010322924074 GBs

2)
2020-02-18 13:24:26 INFO  [main] SenderClient:121 - Uploading time: 14426136205 ns, 14 s
2020-02-18 13:24:26 INFO  [main] SenderClient:124 - Throughput: 567.8582181388741 MBs, 0.5545490411512443 GBs

3) 
2020-02-18 13:24:57 INFO  [main] SenderClient:121 - Uploading time: 14426576044 ns, 14 s
2020-02-18 13:24:57 INFO  [main] SenderClient:124 - Throughput: 567.8409052165254 MBs, 0.554532134000513 GBs


2 ENIs: 

1)
2020-02-18 13:21:53 INFO  [main] SenderClient:121 - Uploading time: 14551813422 ns, 14 s
2020-02-18 13:21:53 INFO  [main] SenderClient:124 - Throughput: 562.9538918919214 MBs, 0.5497596600507045 GBs

2)
2020-02-18 13:22:23 INFO  [main] SenderClient:121 - Uploading time: 14550627932 ns, 14 s
2020-02-18 13:22:23 INFO  [main] SenderClient:124 - Throughput: 562.9997576932063 MBs, 0.5498044508722718 GBs

3) 
2020-02-18 13:22:48 INFO  [main] SenderClient:121 - Uploading time: 14388086326 ns, 14 s
2020-02-18 13:22:48 INFO  [main] SenderClient:124 - Throughput: 569.359942273674 MBs, 0.5560155686266348 GBs
```

### Without actual writings ("fake writings")
```
1 ENI:

1)
2020-02-18 13:31:48 INFO  [main] SenderClient:121 - Uploading time: 11262873654 ns, 11 s
2020-02-18 13:31:48 INFO  [main] SenderClient:124 - Throughput: 727.3454583316415 MBs, 0.7102982991519936 GBs

2)
2020-02-18 13:32:27 INFO  [main] SenderClient:121 - Uploading time: 11043020879 ns, 11 s
2020-02-18 13:32:27 INFO  [main] SenderClient:124 - Throughput: 741.825999403691 MBs, 0.724439452542667 GBs

3) 
2020-02-18 13:32:49 INFO  [main] SenderClient:121 - Uploading time: 11151368954 ns, 11 s
2020-02-18 13:32:49 INFO  [main] SenderClient:124 - Throughput: 734.6183265742926 MBs, 0.7174007095452076 GBs

2 ENIs: 

1)
2020-02-18 13:33:53 INFO  [main] SenderClient:121 - Uploading time: 13431038791 ns, 13 s
2020-02-18 13:33:53 INFO  [main] SenderClient:124 - Throughput: 609.9304847134665 MBs, 0.5956352389779946 GBs

2)
2020-02-18 13:34:20 INFO  [main] SenderClient:121 - Uploading time: 10636757617 ns, 10 s
2020-02-18 13:34:20 INFO  [main] SenderClient:124 - Throughput: 770.1595067755693 MBs, 0.7521088933355169 GBs

3) 
2020-02-18 13:34:46 INFO  [main] SenderClient:121 - Uploading time: 11161117426 ns, 11 s
2020-02-18 13:34:46 INFO  [main] SenderClient:124 - Throughput: 733.9766877567837 MBs, 0.7167741091374841 GBs
```