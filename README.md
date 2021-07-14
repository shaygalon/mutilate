Memcache-perf
=============

Memcache-perf is a memcached load generator designed for high request
rates, good tail-latency measurements, and realistic request stream
generation.

Requirements
============

1. A C++0x compiler
2. libevent2 (get headers and install build-dev for memcached for rest)
3. zeromq 

Tested on ubuntu 14.04,16.04,18.04 x86 64b and ARMv8.

Building
========

    apt-get install libevent-dev libzmq3-dev
    apt-get build-dep memcached
    make

Basic Usage
===========

Type './mcperf -h' for a full list of command-line options.  At
minimum, a server must be specified.

    $ ./mcperf -s localhost
	#type       avg     std     min      p5     p10     p50     p67     p75     p80     p85     p90     p95     p99    p999   p9999
	read       62.2    24.1    54.7    59.1    59.4    61.5    62.3    62.8    63.0    63.3    63.5    63.8    68.8    80.2  1012.5
	update      0.0     0.0     0.0     0.0     0.0     0.0     0.0     0.0     0.0     0.0     0.0     0.0     0.0     0.0     0.0
	op_q        1.0     0.0     1.0     1.0     1.0     1.0     1.1     1.1     1.1     1.1     1.1     1.1     1.1     1.1     1.1

	Total QPS = 16082.2 (80411 / 5.0s)

	Misses = 0 (0.0%)
	Skipped TXs = 0 (0.0%)

	RX   19861517 bytes :    3.8 MB/s
	TX    2894832 bytes :    0.6 MB/s
	CPU Usage Stats (avg/min/max): 2.21%,0.38%,4.05%

mcperf reports the latency (average, minimum, and various
percentiles) for get and set commands, as well as achieved QPS and
network goodput. A separate thread is also keeping track of client CPU usage on the master.
A warning will be issued if the master client CPU usage goes above 95%.
In that case, it is recommended to add more machines as agents.
If verbose (-v) flag is enabled on the an agent, it will report it's cpu usage as well.

To achieve high request rate, you must configure mcperf to use
multiple threads, multiple connections, connection pipelining, or
remote agents.

    $ ./mcperf -s zephyr2-10g -T 24 -c 8
    #type       avg     min     1st     5th    10th    90th    95th    99th
    read      598.8    86.0   437.2   466.6   482.6   977.0  1075.8  1170.6
    update      0.0     0.0     0.0     0.0     0.0     0.0     0.0     0.0
    op_q        1.5     1.0     1.0     1.1     1.1     1.9     1.9     2.0
    
    Total QPS = 318710.8 (1593559 / 5.0s)
    
    Misses = 0 (0.0%)
    
    RX  393609073 bytes :   75.1 MB/s
    TX   57374136 bytes :   10.9 MB/s

Suggested Usage
===============

Real deployments of memcached often handle the requests of dozens,
hundreds, or thousands of front-end clients simultaneously.  However,
by default, mcperf establishes one connection per server and meters
requests one at a time (it waits for a reply before sending the next
request).  This artificially limits throughput (i.e. queries per
second), as the round-trip network latency is almost certainly far
longer than the time it takes for the memcached server to process one
request.

In order to get reasonable benchmark results with mcperf, it needs
to be configured to more accurately portray a realistic client
workload.  In general, this means ensuring that (1) there are a large
number of client connections, (2) there is the potential for a large
number of outstanding requests, and (3) the memcached server saturates
and experiences queuing delay far before mcperf does. I suggest the
following guidelines:

1. Establish more than 50 connections per memcached _server_
thread.
2. Don't exceed more than about 10 connections per _mcperf_ thread.
3. Use multiple mcperf agents in order to achieve (1) and (2).
4. Do not use more mcperf threads than hardware cores/threads.


Here's an example:

    memcached_server$ memcached -t 4 -c 32768
    agent1$ mcperf -T 16 -A
    agent2$ mcperf -T 16 -A
    agent3$ mcperf -T 16 -A
    agent4$ mcperf -T 16 -A
    agent5$ mcperf -T 16 -A
    agent6$ mcperf -T 16 -A
    agent7$ mcperf -T 16 -A
    agent8$ mcperf -T 16 -A
    master$ mcperf -s memcached_server --loadonly
    master$ mcperf -s memcached_server --noload \
        -B -T 16 -Q 1000 -D 4 -C 4 \
        -a agent1 -a agent2 -a agent3 -a agent4 \
        -a agent5 -a agent6 -a agent7 -a agent8 \
        -c 4 -q 200000

This will create 8\*16\*4 = 512 connections total, which is about 128
per memcached server thread.  This ought to be enough outstanding
requests to cause server-side queuing delay, and no possibility of
client-side queuing delay adulterating the latency measurements.

Command-line Options
====================

    mcperf 0.2
    
    Usage: mcperf -s server[@port] [options]
    
    "High-performance" memcached benchmarking tool
    
      -h, --help                    Print help and exit
          --version                 Print version and exit
      -v, --verbose                 Verbosity. Repeat for more verbose.
          --quiet                   Disable log messages.
    
    Basic options:
      -s, --server=STRING           Memcached server hostname[@port].  Repeat to
                                      specify multiple servers.
          --binary                  Use binary memcached protocol instead of ASCII.
      -q, --qps=INT                 Target aggregate QPS. 0 = peak QPS.  
                                      (default=`0')
      -t, --time=INT                Maximum time to run (seconds).  (default=`5')
      -K, --keysize=STRING          Length of memcached keys (distribution).  
                                      (default=`30')
      -V, --valuesize=STRING        Length of memcached values (distribution).  
                                      (default=`200')
      -r, --records=INT             Number of memcached records to use.  If 
                                      multiple memcached servers are given, this 
                                      number is divided by the number of servers.  
                                      (default=`10000')
      -u, --update=FLOAT            Ratio of set:get commands.  (default=`0.0')
    
    Advanced options:
      -U, --username=STRING         Username to use for SASL authentication.
      -P, --password=STRING         Password to use for SASL authentication.
      -T, --threads=INT             Number of threads to spawn.  (default=`1')
          --affinity                Set CPU affinity for threads, round-robin
      -c, --connections=INT         Connections to establish per server.  
                                      (default=`1')
      -d, --depth=INT               Maximum depth to pipeline requests.  
                                      (default=`1')
      -R, --roundrobin              Assign threads to servers in round-robin 
                                      fashion.  By default, each thread connects to 
                                      every server.
      -i, --iadist=STRING           Inter-arrival distribution (distribution).  
                                      Note: The distribution will automatically be 
                                      adjusted to match the QPS given by --qps.  
                                      (default=`exponential')
      -S, --skip                    Skip transmissions if previous requests are 
                                      late.  This harms the long-term QPS average, 
                                      but reduces spikes in QPS after long latency 
                                      requests.
          --moderate                Enforce a minimum delay of ~1/lambda between 
                                      requests.
          --noload                  Skip database loading.
          --loadonly                Load database and then exit.
      -B, --blocking                Use blocking epoll().  May increase latency.
          --no_nodelay              Don't use TCP_NODELAY.
      -w, --warmup=INT              Warmup time before starting measurement.
      -W, --wait=INT                Time to wait after startup to start 
                                      measurement.
          --save=STRING             Record latency samples to given file.
          --search=N:X              Search for the QPS where N-order statistic < 
                                      Xus.  (i.e. --search 95:1000 means find the 
                                      QPS where 95% of requests are faster than 
                                      1000us).
          --scan=min:max:step       Scan latency across QPS rates from min to max.
    
    Agent-mode options:
      -A, --agentmode               Run client in agent mode.
      -a, --agent=host              Enlist remote agent.
      -p, --agent_port=STRING       Agent port.  (default=`5556')
      -l, --lambda_mul=INT          Lambda multiplier.  Increases share of QPS for 
                                      this client.  (default=`1')
      -C, --measure_connections=INT Master client connections per server, overrides 
                                      --connections.
      -Q, --measure_qps=INT         Explicitly set master client QPS, spread across 
                                      threads and connections.
      -D, --measure_depth=INT       Set master client connection depth.
    
    The --measure_* options aid in taking latency measurements of the
    memcached server without incurring significant client-side queuing
    delay.  --measure_connections allows the master to override the
    --connections option.  --measure_depth allows the master to operate as
    an "open-loop" client while other agents continue as a regular
    closed-loop clients.  --measure_qps lets you modulate the QPS the
    master queries at independent of other clients.  This theoretically
    normalizes the baseline queuing delay you expect to see across a wide
    range of --qps values.
    
    Some options take a 'distribution' as an argument.
    Distributions are specified by <distribution>[:<param1>[,...]].
    Parameters are not required.  The following distributions are supported:
    
       [fixed:]<value>              Always generates <value>.
       uniform:<max>                Uniform distribution between 0 and <max>.
       normal:<mean>,<sd>           Normal distribution.
       exponential:<lambda>         Exponential distribution.
       pareto:<loc>,<scale>,<shape> Generalized Pareto distribution.
       gev:<loc>,<scale>,<shape>    Generalized Extreme Value distribution.
    
       To recreate the Facebook "ETC" request stream from [1], the
       following hard-coded distributions are also provided:
    
       fb_value   = a hard-coded discrete and GPareto PDF of value sizes
       fb_key     = "gev:30.7984,8.20449,0.078688", key-size distribution
       fb_ia      = "pareto:0.0,16.0292,0.154971", inter-arrival time dist.
    
    [1] Berk Atikoglu et al., Workload Analysis of a Large-Scale Key-Value Store,
        SIGMETRICS 2012
    

	mcperf 0.3

	Usage: mcperf -s server[@port] [options]

	"High-performance" memcached benchmarking tool

	  -h, --help                    Print help and exit
		  --version                 Print version and exit
	  -v, --verbose                 Verbosity. Repeat for more verbose.
		  --quiet                   Disable log messages.

	Basic options:
	  -s, --server=STRING           Memcached server hostname[@port[-end_port]].
									  Repeat to specify multiple servers. 
		  --binary                  Use binary memcached protocol instead of ASCII.
	  -q, --qps=INT                 Target aggregate QPS. 0 = peak QPS.
									  (default=`0')
	  -t, --time=INT                Maximum time to run (seconds).  (default=`5')
		  --profile=INT             Select one of several predefined profiles.
	  -K, --keysize=STRING          Length of memcached keys (distribution).
									  (default=`30')
		  --keyorder=STRING         Selection of memcached keys to use
									  (distribution).  (default=`none')
	  -V, --valuesize=STRING        Length of memcached values (distribution).
									  (default=`200')
	  -r, --records=INT             Number of memcached records to use.  If
									  multiple memcached servers are given, this
									  number is divided by the number of servers.
									  (default=`10000')
	  -u, --update=FLOAT            Ratio of set:get commands.  (default=`0.0')

	Advanced options:
	  -U, --username=STRING         Username to use for SASL authentication.
	  -P, --password=STRING         Password to use for SASL authentication.
	  -T, --threads=INT             Number of threads to spawn.  (default=`1')
		  --affinity                Set CPU affinity for threads, round-robin
	  -c, --connections=INT         Connections to establish per server.
									  (default=`1')
	  -d, --depth=INT               Maximum depth to pipeline requests.
									  (default=`1')
	  -R, --roundrobin              Assign threads to servers in round-robin
									  fashion.  By default, each thread connects to
									  every server.
	  -i, --iadist=STRING           Inter-arrival distribution (distribution).
									  Note: The distribution will automatically be
									  adjusted to match the QPS given by --qps.
									  (default=`exponential')
	  -S, --skip                    Skip transmissions if previous requests are
									  late.  This harms the long-term QPS average,
									  but reduces spikes in QPS after long latency
									  requests.
		  --moderate                Enforce a minimum delay of ~1/lambda between
									  requests.
		  --noload                  Skip database loading.
		  --loadonly                Load database and then exit.
	  -B, --blocking                Use blocking epoll().  May increase latency.
		  --no_nodelay              Don't use TCP_NODELAY.
	  -w, --warmup=INT              Warmup time before starting measurement.
	  -W, --wait=INT                Time to wait after startup to start
									  measurement.
		  --save=STRING             Record latency samples to given file.
		  --search=N:X              Search for the QPS where N-order statistic <
									  Xus.  (i.e. --search 95:1000 means find the
									  QPS where 95% of requests are faster than
									  1000us).
		  --scan=min:max:step       Scan latency across QPS rates from min to max.
	  -e, --trace                   To enable server tracing based on client
									  activity, will issue special
									  start_trace/stop_trace commands. Requires
									  memcached to support these commands.
	  -G, --getq_size=INT           Size of queue for multiget requests.
									  (default=`100')
	  -g, --getq_freq=FLOAT         Frequency of multiget requests, 0 for no
									  multi-get, 100 for only multi-get.
									  (default=`0.0')
		  --keycache_capacity=INT   Cached key capacity. (default 10000)
									  (default=`10000')
		  --keycache_reuse=INT      Number of times to reuse key cache before
									  generating new req sequence. (Default 100)
									  (default=`100')
		  --keycache_regen=INT      When regenerating control number of requests to
									  regenerate. (Default 1%)  (default=`1')
		  --plot_all                Create plot/csv of latency histogram at each
									  step when using gnuplot and loghistogram
									  sampler

	Agent-mode options:
	  -A, --agentmode               Run client in agent mode.
	  -a, --agent=host              Enlist remote agent.
	  -p, --agent_port=STRING       Agent port.  (default=`5556')
	  -l, --lambda_mul=INT          Lambda multiplier.  Increases share of QPS for
									  this client.  (default=`1')
	  -C, --measure_connections=INT Master client connections per server, overrides
									  --connections.
	  -Q, --measure_qps=INT         Explicitly set master client QPS, spread across
									  threads and connections.
	  -D, --measure_depth=INT       Set master client connection depth.
	  -m, --poll_freq=INT           Set frequency in seconds for agent protocol
									  recv polling.  (default=`1')
	  -M, --poll_max=INT            Set timeout for agent protocol recv polling. An
									  agent not responding within time limit will
									  be dropped.  (default=`120')

	The --measure_* options aid in taking latency measurements of the
	memcached server without incurring significant client-side queuing
	delay.  --measure_connections allows the master to override the
	--connections option.  --measure_depth allows the master to operate as
	an "open-loop" client while other agents continue as a regular
	closed-loop clients.  --measure_qps lets you modulate the QPS the
	master queries at independent of other clients.  This theoretically
	normalizes the baseline queuing delay you expect to see across a wide
	range of --qps values.

	Predefined profiles to approximate some use cases:
	1. memcached for web serving benchmark : p95, 20ms, FB key/value/IA, >4000
	connections to the device under test.
	2. memcached for applications backends : p99, 10ms, 32B key , 1000B value,
	uniform IA,  >1000 connections
	3. memcached for low latency (e.g. stock trading): p99.9, 32B key, 200B value,
	uniform IA, QPS rate set to 100000	
	4. P99.9, 1 msec. Key size = 32 bytes; value size has uniform distribution from
	100 bytes to 1k; 

	Some options take a 'distribution' as an argument.
	Distributions are specified by <distribution>[:<param1>[,...]].
	Parameters are not required.  The following distributions are supported:

	   [fixed:]<value>              Always generates <value>.
	   uniform:<max>                Uniform distribution between 0 and <max>.
	   normal:<mean>,<sd>           Normal distribution.
	   exponential:<lambda>         Exponential distribution.
	   pareto:<loc>,<scale>,<shape> Generalized Pareto distribution.
	   gev:<loc>,<scale>,<shape>    Generalized Extreme Value distribution.

	   To recreate the Facebook "ETC" request stream from [1], the
	   following hard-coded distributions are also provided:

	   fb_value   = a hard-coded discrete and GPareto PDF of value sizes
	   fb_key     = "gev:30.7984,8.20449,0.078688", key-size distribution
	   fb_ia      = "pareto:0.0,16.0292,0.154971", inter-arrival time dist.

	[1] Berk Atikoglu et al., Workload Analysis of a Large-Scale Key-Value Store,
		SIGMETRICS 2012


mcperf 0.3

Usage: mcperf -s server[@port] [options]

"High-performance" memcached benchmarking tool

  -h, --help                    Print help and exit
      --version                 Print version and exit
  -v, --verbose                 Verbosity. Repeat for more verbose.
      --quiet                   Disable log messages.

Basic options:
  -s, --server=STRING           Memcached server hostname[:port[-end_port]].
                                  Repeat to specify multiple servers. 
      --binary                  Use binary memcached protocol instead of ASCII.
  -q, --qps=INT                 Target aggregate QPS. 0 = peak QPS.
                                  (default=`0')
  -t, --time=INT                Maximum time to run (seconds).  (default=`5')
      --profile=INT             Select one of several predefined profiles.
  -K, --keysize=STRING          Length of memcached keys (distribution).
                                  (default=`30')
      --keyorder=STRING         Selection of memcached keys to use
                                  (distribution).  (default=`none')
  -V, --valuesize=STRING        Length of memcached values (distribution).
                                  (default=`200')
  -r, --records=INT             Number of memcached records to use.  If
                                  multiple memcached servers are given, this
                                  number is divided by the number of servers.
                                  (default=`10000')
  -u, --update=FLOAT            Ratio of set:get commands.  (default=`0.0')

Advanced options:
  -U, --username=STRING         Username to use for SASL authentication.
  -P, --password=STRING         Password to use for SASL authentication.
  -T, --threads=INT             Number of threads to spawn.  (default=`1')
      --affinity                Set CPU affinity for threads, round-robin
  -c, --connections=INT         Connections to establish per server.
                                  (default=`1')
  -d, --depth=INT               Maximum depth to pipeline requests.
                                  (default=`1')
  -R, --roundrobin              Assign threads to servers in round-robin
                                  fashion.  By default, each thread connects to
                                  every server.
  -i, --iadist=STRING           Inter-arrival distribution (distribution).
                                  Note: The distribution will automatically be
                                  adjusted to match the QPS given by --qps.
                                  (default=`exponential')
  -S, --skip                    Skip transmissions if previous requests are
                                  late.  This harms the long-term QPS average,
                                  but reduces spikes in QPS after long latency
                                  requests.
      --moderate                Enforce a minimum delay of ~1/lambda between
                                  requests.
      --noload                  Skip database loading.
      --loadonly                Load database and then exit.
  -B, --blocking                Use blocking epoll().  May increase latency.
      --no_nodelay              Don't use TCP_NODELAY.
  -w, --warmup=INT              Warmup time before starting measurement.
  -W, --wait=INT                Time to wait after startup to start
                                  measurement.
      --save=STRING             Record latency samples to given file.
      --search=N:X              Search for the QPS where N-order statistic <
                                  Xus.  (i.e. --search 95:1000 means find the
                                  QPS where 95% of requests are faster than
                                  1000us).
      --scan=min:max:step       Scan latency across QPS rates from min to max.
  -e, --trace                   To enable server tracing based on client
                                  activity, will issue special
                                  start_trace/stop_trace commands. Requires
                                  memcached to support these commands.
  -G, --getq_size=INT           Size of queue for multiget requests.
                                  (default=`100')
  -g, --getq_freq=FLOAT         Frequency of multiget requests, 0 for no
                                  multi-get, 100 for only multi-get.
                                  (default=`0.0')
      --keycache_capacity=INT   Cached key capacity. (default 10000)
                                  (default=`10000')
      --keycache_reuse=INT      Number of times to reuse key cache before
                                  generating new req sequence. (Default 100)
                                  (default=`100')
      --keycache_regen=INT      When regenerating control number of requests to
                                  regenerate. (Default 1%)  (default=`1')
      --plot_all                Create plot/csv of latency histogram at each
                                  step when using gnuplot and loghistogram
                                  sampler

Agent-mode options:
  -A, --agentmode               Run client in agent mode.
  -a, --agent=host              Enlist remote agent.
  -p, --agent_port=STRING       Agent port.  (default=`5556')
  -l, --lambda_mul=INT          Lambda multiplier.  Increases share of QPS for
                                  this client.  (default=`1')
  -C, --measure_connections=INT Master client connections per server, overrides
                                  --connections.
  -Q, --measure_qps=INT         Explicitly set master client QPS, spread across
                                  threads and connections.
  -D, --measure_depth=INT       Set master client connection depth.
  -m, --poll_freq=INT           Set frequency in seconds for agent protocol
                                  recv polling.  (default=`1')
  -M, --poll_max=INT            Set timeout for agent protocol recv polling. An
                                  agent not responding within time limit will
                                  be dropped.  (default=`120')

The --measure_* options aid in taking latency measurements of the
memcached server without incurring significant client-side queuing
delay.  --measure_connections allows the master to override the
--connections option.  --measure_depth allows the master to operate as
an "open-loop" client while other agents continue as a regular
closed-loop clients.  --measure_qps lets you modulate the QPS the
master queries at independent of other clients.  This theoretically
normalizes the baseline queuing delay you expect to see across a wide
range of --qps values.

Predefined profiles to approximate some use cases:
1. memcached for web serving benchmark : p95, 20ms, FB key/value/IA, >4000
connections to the device under test.
2. memcached for applications backends : p99, 10ms, 32B key , 1000B value,
uniform IA,  >1000 connections
3. memcached for low latency (e.g. stock trading): p99.9, 32B key, 200B value,
uniform IA, QPS rate set to 100000	
4. P99.9, 1 msec. Key size = 32 bytes; value size has uniform distribution from
100 bytes to 1k; 

Some options take a 'distribution' as an argument.
Distributions are specified by <distribution>[:<param1>[,...]].
Parameters are not required.  The following distributions are supported:

   [fixed:]<value>              Always generates <value>.
   uniform:<max>                Uniform distribution between 0 and <max>.
   normal:<mean>,<sd>           Normal distribution.
   exponential:<lambda>         Exponential distribution.
   pareto:<loc>,<scale>,<shape> Generalized Pareto distribution.
   gev:<loc>,<scale>,<shape>    Generalized Extreme Value distribution.

   To recreate the Facebook "ETC" request stream from [1], the
   following hard-coded distributions are also provided:

   fb_value   = a hard-coded discrete and GPareto PDF of value sizes
   fb_key     = "gev:30.7984,8.20449,0.078688", key-size distribution
   fb_ia      = "pareto:0.0,16.0292,0.154971", inter-arrival time dist.

[1] Berk Atikoglu et al., Workload Analysis of a Large-Scale Key-Value Store,
    SIGMETRICS 2012

