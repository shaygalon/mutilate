#include <arpa/inet.h>
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <queue>
#include <string>
#include <vector>

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/dns.h>
#include <event2/event.h>
#include <event2/thread.h>
#include <event2/util.h>

#include "config.h"

#ifdef HAVE_LIBZMQ
#include <zmq.hpp>
#endif

#include "AdaptiveSampler.h"
#include "AgentStats.h"
#ifndef HAVE_PTHREAD_BARRIER_INIT
#include "barrier.h"
#endif
#include "cmdline.h"
#include "Connection.h"
#include "ConnectionOptions.h"
#include "log.h"
#include "mcperf.h"
#include "util.h"
#include "cpu_stat_thread.h"

#define MIN(a,b) ((a) < (b) ? (a) : (b))

using namespace std;

gengetopt_args_info args;
char random_char[2 * 1024 * 1024];  // Buffer used to generate random values.

#ifdef HAVE_LIBZMQ
vector<zmq::socket_t*> agent_sockets;
zmq::context_t context(1);
#endif

struct thread_data {
  const vector<string> *servers;
  options_t *options;
  bool master;  // Thread #0, not to be confused with agent master.
#ifdef HAVE_LIBZMQ
  zmq::socket_t *socket;
#endif
};

// struct evdns_base *evdns;

pthread_barrier_t barrier;

double boot_time;

void init_random_stuff();

void go(const vector<string> &servers, options_t &options,
        ConnectionStats &stats
#ifdef HAVE_LIBZMQ
, zmq::socket_t* socket = NULL
#endif
);

char const *command_string[] = {
	"start_trace",
	"stop_trace",
	"shutdown"
};

void do_mcperf(const vector<string> &servers, options_t &options,
                 ConnectionStats &stats, bool master = true
#ifdef HAVE_LIBZMQ
, zmq::socket_t* socket = NULL
#endif
);
void args_to_options(options_t* options);
void* thread_main(void *arg);

#ifdef HAVE_LIBZMQ
static unsigned int max_poll_time=0;
static unsigned int poll_interval_s=1;
//static long poll_interval_ns=100000;
void setup_socket_timers() {
	if (args.poll_freq_given)
		poll_interval_s=args.poll_freq_arg;
	if (args.poll_max_given)
		max_poll_time=args.poll_max_arg;
}

#ifdef ZMQ_CPP11
const zmq::recv_flags recv_noblock_flag=zmq::recv_flags::dontwait;
const zmq::send_flags send_noblock_flag=zmq::send_flags::dontwait;
#else
#if defined(ZMQ_NOBLOCK)
const int recv_noblock_flag=ZMQ_NOBLOCK;
const int send_noblock_flag=ZMQ_NOBLOCK;
#elif defined(ZMQ_DONTWAIT)
const int recv_noblock_flag=ZMQ_DONTWAIT;
const int send_noblock_flag=ZMQ_DONTWAIT;
#endif
#endif

void tokenize(const string& str,
                      vector<string>& tokens,
                      const string& delimiters = ",")
{
    // Skip delimiters at beginning.
    string::size_type lastPos = str.find_first_not_of(delimiters, 0);
    // Find first "non-delimiter".
    string::size_type pos     = str.find_first_of(delimiters, lastPos);

    while (string::npos != pos || string::npos != lastPos)
    {
        // Found a token, add it to the vector.
        tokens.push_back(str.substr(lastPos, pos - lastPos));
        // Skip delimiters.  Note the "not_of"
        lastPos = str.find_first_not_of(delimiters, pos);
        // Find next "non-delimiter"
        pos = str.find_first_of(delimiters, lastPos);
    }
}
void deTokenize(string& str,
                      const vector<string>& tokens,
                      const string& delimiter = ",")
{
    vector<string>::const_iterator it;
	//collect all tokens to a single string
	it=tokens.begin();
	str=*it;
	it++;
    for ( ; 
	 it != tokens.end() ; 
	 it++) 
	 {
		str += delimiter;
		str += *it;
	 }
}

bool poll_recv(zmq::socket_t& socket, zmq::message_t& msg) {
	unsigned int timer=max_poll_time;
	bool status=false;
	D("- recv"); 
	//if no timeout, just block
	if (timer == 0) { 
#ifdef ZMQ_CPP11
		auto recv_result = socket.recv(msg);
		return recv_result.has_value();
#else
		return socket.recv(msg);
#endif
	}
	//otherwise, recv non blocking until timeout passed
	int itid=0;
	struct timespec st;
	st.tv_sec=poll_interval_s;
	st.tv_nsec=0;
	do {
		itid++;
#ifdef ZMQ_CPP11
		auto recv_result = socket.recv(msg, recv_noblock_flag);
		if (recv_result.has_value())
			return true;
#else
		status = socket.recv(msg, recv_noblock_flag);
		if (status)
			return status;
#endif
		// if failed, check errno for fail reason
		if (zmq_errno () == EAGAIN) {
			// if failed just bcs msg non blocking, try again after sleeping for poll interval
			nanosleep(&st,NULL);
			timer-=poll_interval_s;
		} else {
			W("ERROR in socket! [%d]",zmq_errno());
			break;
		}
		if ((itid & 0xff) == 0) {
			V("Socket recv multi iterate...");//TODO:DBG
		}
	} while (timer > poll_interval_s);
	W("Failed to recv within requested time limit. Aborting recv.");
	return false;
}

bool poll_send(zmq::socket_t& socket, zmq::message_t& msg) {
	unsigned int timer=max_poll_time;
	bool status=false;
	D("- send"); 
	//if no timeout, just block
#if 1
#ifdef ZMQ_CPP11
	auto send_result = socket.send(msg, zmq::send_flags::none);
	return send_result.has_value();
#else
	return socket.send(msg);
#endif
#else
	if (timer == 0) { 
		return socket.send(msg);
	}
	//otherwise, recv non blocking until timeout passed
	do {
		status=socket.send(msg, send_noblock_flag);
		if (status == 0)
			return status;
		// if failed, check errno for fail reason
		if (zmq_errno () == EAGAIN) {
			// if failed just bcs msg non blocking, try again after sleeping for poll interval
			usleep(poll_interval_us);
			timer-=poll_interval_us;
		} else {
			W("ERROR in socket! [%d]",zmq_errno());
			break;
		}
	} while (timer > poll_interval_us);
	W("Failed to send within requested time limit. Aborting send.");
	return false;
#endif
}

static std::string s_recv (zmq::socket_t &socket) {
  zmq::message_t message;
  //socket.recv(&message);
  bool status=poll_recv(socket, message);

  if (status)
	return std::string(static_cast<char*>(message.data()), message.size());
  else 
	return std::string("FAIL-RECV");
}

//  Convert string to 0MQ string and send to socket
static bool s_send (zmq::socket_t &socket, const std::string &string) {
  zmq::message_t message(string.size());
  memcpy(message.data(), string.data(), string.size());

  //return socket.send(message);
  return poll_send(socket,message);
}

/*
 * Agent protocol
 *
 * PREPARATION PHASE
 *
 * 1. Master -> Agent: options_t
 *
 * options_t contains most of the information needed to drive the
 * client, including the aggregate QPS that has been requested.
 * However, neither the master nor the agent know at this point how
 * many total connections will be made to the memcached server.
 *
 * 2. Agent -> Master: int num = (--threads) * (--lambda_mul)
 *
 * The agent sends a number to the master indicating how many threads
 * this mcperf agent will spawn, and a mutiplier that weights how
 * many QPS this agent's connections will send relative to unweighted
 * connections (i.e. we can request that a purely load-generating
 * agent or an agent on a really fast network connection be more
 * aggressive than other agents or the master).
 *
 * 3. Master -> Agent: lambda_denom
 *
 * The master aggregates all of the numbers collected in (2) and
 * computes a global "lambda_denom".  Which is essentially a count of
 * the total number of Connections across all mcperf instances,
 * weighted by lambda_mul if necessary.  It broadcasts this number to
 * all agents.
 *
 * Each instance of mcperf at this point adjusts the lambda in
 * options_t sent in (1) to account for lambda_denom.  Note that
 * lambda_mul is specific to each instance of mcperf
 * (i.e. --lambda_mul X) and not sent as part of options_t.
 *
 *   lambda = qps / lambda_denom * args.lambda_mul;
 *
 * RUN PHASE
 *
 * After the PREP phase completes, everyone executes do_mcperf().
 * All clients spawn threads, open connections, load the DB, and wait
 * for all connections to become IDLE.  Following that, they
 * synchronize and finally do the heavy lifting.
 * 
 * [IF WARMUP] -1:  Master <-> Agent: Synchronize
 * [IF WARMUP]  0:  Everyone: RUN for options.warmup seconds.
 * 1. Master <-> Agent: Synchronize
 * 2. Everyone: RUN for options.time seconds.
 * 3. Master -> Agent: Dummy message
 * 4. Agent -> Master: Send AgentStats [w/ RX/TX bytes, # gets/sets]
 *
 * The master then aggregates AgentStats across all agents with its
 * own ConnectionStats to compute overall statistics.
 */

void agent() {
  zmq::context_t context(1);

  zmq::socket_t socket(context, ZMQ_REP);
  socket.bind((string("tcp://*:")+string(args.agent_port_arg)).c_str());

int lid=0;
  while (true) {
    zmq::message_t request;

#ifdef ZMQ_CPP11
    auto recv_result = socket.recv(request);
#else
    socket.recv(&request);
#endif
lid++;

    zmq::message_t num(sizeof(int));
    *((int *) num.data()) = args.threads_arg * args.lambda_mul_arg;
#ifdef ZMQ_CPP11
    socket.send(num, zmq::send_flags::none);
#else
    socket.send(num);
#endif
V("sent num %d",lid);
    options_t options;
    memcpy(&options, request.data(), sizeof(options));
V("Got options: %d %s",options.connections,options.loadonly ? "loadonly" : options.noload ? "noload" : "");

	//get a string containing the servers, and parse it to extract all servers
	string server_opt=s_recv(socket);
    vector<string> servers;
	tokenize(server_opt,servers);
    s_send(socket, "ack");
V("sent ack");
    vector<string>::iterator i;

    for (i= servers.begin(); i!=servers.end(); i++) {
      V("Got server = %s", i->c_str());
    }

    options.threads = args.threads_arg;

#ifdef ZMQ_CPP11
    recv_result = socket.recv(request);
#else
    socket.recv(&request);
#endif
    options.lambda_denom = *((int *) request.data());
    s_send(socket, "THANKS");
V("sent tnx");

    //    V("AGENT SLEEPS"); sleep(1);
    options.lambda = (double) options.qps / options.lambda_denom * args.lambda_mul_arg;

    V("lambda_denom = %d, lambda = %f, qps = %d",
      options.lambda_denom, options.lambda, options.qps);

    //    if (options.threads > 1)
      pthread_barrier_init(&barrier, NULL, options.threads);

    ConnectionStats stats = ConnectionStats();
V("launching go");

    go(servers, options, stats, &socket);
V("Done run.");
    AgentStats as;

    as.rx_bytes = stats.rx_bytes;
    as.tx_bytes = stats.tx_bytes;
    as.gets = stats.gets;
    as.sets = stats.sets;
    as.get_misses = stats.get_misses;
    as.start = stats.start;
    as.stop = stats.stop;
    as.skips = stats.skips;
#ifdef LOGSAMPLER_BINS
	for (int i=0; i<LOGSAMPLER_BINS; i++) 
		as.get_bins[i]=stats.get_sampler.bins[i];
	as.get_sum = stats.get_sampler.sum;
	as.get_sum_sq = stats.get_sampler.sum_sq;
#endif	

    string req = s_recv(socket);
    V("req = %s", req.c_str());
    request.rebuild(sizeof(as));
    memcpy(request.data(), &as, sizeof(as));
#ifdef ZMQ_CPP11
    socket.send(request, zmq::send_flags::none);
#else
    socket.send(request);
#endif
    V("send = %s", req.c_str());
	if (log_level > DEBUG) {
		stats.print_header(false);
		printf(" QPS\n");
		stats.print_stats("read",   stats.get_sampler,false);
		printf(" %8.1f\n", stats.get_qps());
	}

  }
}

void prep_agent(const vector<string>& servers, options_t& options) {
  int sum = options.lambda_denom;
  int aid=0;
  if (args.measure_connections_given)
    sum = args.measure_connections_arg * options.server_given * options.threads;

  int master_sum = sum;
  if (args.measure_qps_given) {
    sum = 0;
    if (options.qps) options.qps -= args.measure_qps_arg;
  }
  vector<zmq::socket_t*>::iterator its;
  for ( its=agent_sockets.begin(); its!=agent_sockets.end(); its++ ) {
    zmq::socket_t *s=*its;
    zmq::message_t message(sizeof(options_t));
	bool status;
	aid++;

V("Agent %d prep ", aid);
    memcpy((void *) message.data(), &options, sizeof(options_t));
    status=poll_send(*s,message);
D("Agent %d prep send = %s", aid, status?"true":"false");

    zmq::message_t rep;
    status=poll_recv(*s, rep);
D("Agent %d prep recv= %s", aid, status?"true":"false");
	if (!status) { // problem communicating with this agent, don't use it!
		W("Agent failure detected, skip agent %d!",aid);
		its=agent_sockets.erase(its); // remove from list of active agents
		delete(s);
		its--; // adjust the iterator since we did not iterate over the next agent
		continue;
	}
    unsigned int num = *((int *) rep.data());

    sum += options.connections * (options.roundrobin ?
            (servers.size() > num ? servers.size() : num) : 
            (servers.size() * num));
	int itid=0;
	//collect all servers to a single msg
	string all_servers;
	deTokenize(all_servers,servers);
	//send servers msg to agent and wait for ack
	s_send(*s, all_servers);
	string response = s_recv(*s);
	if (response.compare("FAIL-RECV") == 0) {
		itid=-1;
	}

	// in case communication with agent broke down, remove from active list
	if (itid<0) {
		W("Agent failure detected, skip agent %d!",aid);
		its=agent_sockets.erase(its); // remove from list of active agents
		delete(s);
		its--; // adjust the iterator since we did not iterate over the next agent
	}
  }

  
  // Adjust options_t according to --measure_* arguments.
  options.lambda_denom = sum;
  options.lambda = (double) options.qps / options.lambda_denom *
    args.lambda_mul_arg;

  V("lambda_denom = %d", sum);

  if (args.measure_qps_given) {
    double master_lambda = (double) args.measure_qps_arg / master_sum;

    if (options.qps && master_lambda > options.lambda)
      V("warning: master_lambda (%f) > options.lambda (%f)",
        master_lambda, options.lambda);

    options.lambda = master_lambda;
  }

  if (args.measure_depth_given) options.depth = args.measure_depth_arg;

  for ( its=agent_sockets.begin(); its!=agent_sockets.end(); its++ ) {
    zmq::socket_t *s=*its;
    zmq::message_t message(sizeof(sum));
    *((int *) message.data()) = sum;
    poll_send(*s,message);
    string rep = s_recv(*s);
	if (rep.compare("FAIL-RECV") == 0) {
		W("Agent failure detected, skip agent %d!",aid);
		its=agent_sockets.erase(its); // remove from list of active agents
		delete(s);
		its--; // adjust the iterator since we did not iterate over the next agent
	}
  }

  // Master sleeps here to give agents a chance to connect to
  // memcached server before the master, so that the master is never
  // the very first set of connections.  Is this reasonable or
  // necessary?  Most probably not.
  V("MASTER SLEEPS"); sleep_time(1.5);
}

void finish_agent(ConnectionStats &stats) {
	int aid=0;
  //for (auto s: agent_sockets) {
  vector<zmq::socket_t*>::iterator its;
  for ( its=agent_sockets.begin(); its!=agent_sockets.end(); its++ ) {
    zmq::socket_t *s=*its;
	aid++;
	bool status;
    status=s_send(*s, "stats");
D("Agent %d finish send = %s", aid, status?"true":"false");

    AgentStats as;
    zmq::message_t message;

    status=poll_recv(*s, message);
D("Agent %d finish recv = %s", aid, status?"true":"false");
    memcpy(&as, message.data(), sizeof(as));
    stats.accumulate(as);
  }
}

/*
 * This synchronization routine is ridiculous because the master only
 * has a ZMQ_REQ socket to the agents, but it needs to wait for a
 * message from each agent before it releases them.  In order to get
 * the ZMQ socket into a state where it'll allow the agent to send it
 * a message, it must first send a message ("sync_req").  In order to
 * not leave the socket dangling with an incomplete transaction, the
 * agent must send a reply ("ack").
 *
 * Without this stupid complication it would be:
 *
 * For each agent:
 *   Agent -> Master: sync
 * For each agent:
 *   Master -> Agent: proceed
 *
 * In this way, all agents must arrive at the barrier and the master
 * must receive a message from each of them before it continues.  It
 * then broadcasts the message to proceed, which reasonably limits
 * skew.
 */

int sync_agent(zmq::socket_t* socket) {
  V("agent: synchronizing");
  int aid=0;
  int errors=0;
  if (args.agent_given) {
   vector<zmq::socket_t*>::iterator its;
   for ( its=agent_sockets.begin(); its!=agent_sockets.end(); its++ ) {
    zmq::socket_t *s=*its;
      s_send(*s, "sync_req");
	  D("Sent sync_req to agent %d",++aid);
   }
	aid=0;

    /* The real sync */
   for ( its=agent_sockets.begin(); its!=agent_sockets.end(); its++ ) {
	zmq::socket_t *s=*its;aid++;
    string rep = s_recv(*s);
    if (rep.compare(string("sync")) != 0) {
        W("sync_agent[M]: out of sync [1] for agent %d expected sync got %s",aid,rep.c_str());
		errors++;
		if (rep.compare("FAIL-RECV") == 0) {
			W("Agent failure detected, skip agent %d!",aid);
			its=agent_sockets.erase(its); // remove from list of active agents
			delete(s);
			its--; // adjust the iterator since we did not iterate over the next agent
		}

	}
   }
	aid=0;
   for ( its=agent_sockets.begin(); its!=agent_sockets.end(); its++ ) {
    zmq::socket_t *s=*its;
      s_send(*s, "proceed");
	D("Sent proceed to agent %d",++aid);
   }
    /* End sync */
	aid=0;
   for ( its=agent_sockets.begin(); its!=agent_sockets.end(); its++ ) {
    zmq::socket_t *s=*its;aid++;
    string rep = s_recv(*s);
    if (rep.compare(string("ack")) != 0) {
        W("sync_agent[M]: out of sync [2] for agent %d expected ack got %s",aid,rep.c_str());
		errors++;
		if (rep.compare("FAIL-RECV") == 0) {
			W("Agent failure detected, skip agent %d!",aid);
			its=agent_sockets.erase(its); // remove from list of active agents
			delete(s);
			its--; // adjust the iterator since we did not iterate over the next agent
		}
	}
   }
  } else if (args.agentmode_given) {
      string rep = s_recv(*socket);
     if (rep.compare(string("sync_req")) != 0) {
      W("sync_agent[A]: out of sync [1] got %s expected sync_req",rep.c_str());
	errors++;
	}

    /* The real sync */
     s_send(*socket, "sync");
     rep = s_recv(*socket);
     if (rep.compare(string("proceed")) != 0) {
      W("sync_agent[A]: out of sync [2] got %s expected proceed",rep.c_str());
	errors++;
	}
    /* End sync */

    s_send(*socket, "ack");
  }

  V("agent: synchronized with %d errors",errors);
  return errors;
}
#endif

string name_to_ipaddr(string host, int addport=1) {
  char *s_copy = new char[host.length() + 1];
  strcpy(s_copy, host.c_str());

  char *saveptr = NULL;  // For reentrant strtok().

  char *h_ptr = strtok_r(s_copy, "@", &saveptr);
  char *p_ptr = strtok_r(NULL, "@", &saveptr);

  char ipaddr[16];

  if (h_ptr == NULL)
    DIE("strtok(.., \":\") failed to parse %s", host.c_str());

  string hostname = h_ptr;
  string port = "11211";
  if (p_ptr) port = p_ptr;

  struct evutil_addrinfo hints;
  struct evutil_addrinfo *answer = NULL;
  int err;

  /* Build the hints to tell getaddrinfo how to act. */
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC; /* v4 or v6 is fine. */
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP; /* We want a TCP socket */
  /* Only return addresses we can use. */
  hints.ai_flags = EVUTIL_AI_ADDRCONFIG;

  /* Look up the hostname. */
  err = evutil_getaddrinfo(h_ptr, NULL, &hints, &answer);
  if (err < 0) {
    DIE("Error while resolving '%s': %s",
        host.c_str(), evutil_gai_strerror(err));
  }

  if (answer == NULL) DIE("No DNS answer.");

  void *ptr = NULL;
  switch (answer->ai_family) {
  case AF_INET:
    ptr = &((struct sockaddr_in *) answer->ai_addr)->sin_addr;
    break;
  case AF_INET6:
    ptr = &((struct sockaddr_in6 *) answer->ai_addr)->sin6_addr;
    break;
  }

  inet_ntop (answer->ai_family, ptr, ipaddr, 16);

  D("Resolved %s to %s", h_ptr, (string(ipaddr) + ":" + string(port)).c_str());

  delete[] s_copy;
  if (addport)
	return string(ipaddr) + "@" + string(port);
  else
	return string(ipaddr);
}

int main(int argc, char **argv) {
  if (cmdline_parser(argc, argv, &args) != 0) exit(-1);

  for (unsigned int i = 0; i < args.verbose_given; i++)
    log_level = (log_level_t) ((int) log_level - 1);

  if (args.quiet_given) log_level = QUIET;

  if (args.depth_arg < 1) DIE("--depth must be >= 1");
  if (args.qps_arg < 0) DIE("--qps must be >= 0");
  if (args.update_arg < 0.0 || args.update_arg > 1.0)
    DIE("--update must be >= 0.0 and <= 1.0");
  if (args.time_arg < 1) DIE("--time must be >= 1");
  if (args.connections_arg < 1 || args.connections_arg > MAXIMUM_CONNECTIONS)
    DIE("--connections must be between [1,%d]", MAXIMUM_CONNECTIONS);
  if (!args.server_given && !args.agentmode_given)
    DIE("--server or --agentmode must be specified.");

  // TODO: Discover peers, share arguments.

  init_random_stuff();
  boot_time = get_time();
  setvbuf(stdout, NULL, _IONBF, 0);

  //  struct event_base *base;

  //  if ((base = event_base_new()) == NULL) DIE("event_base_new() fail");
  //  evthread_use_pthreads();

  //  if ((evdns = evdns_base_new(base, 1)) == 0) DIE("evdns");
  options_t options;
  bzero(&options, sizeof(options_t));
  args_to_options(&options);

#ifdef HAVE_LIBZMQ
  if (args.agentmode_given) {
    agent();
    return 0;
  } else if (args.agent_given) {
	int status;
	setup_socket_timers();
    for (unsigned int i = 0; i < args.agent_given; i++) {
      zmq::socket_t *s = new zmq::socket_t(context, ZMQ_REQ);
	  if (s==NULL) {
		DIE("Could not open socket! %s",zmq_strerror(zmq_errno()));
	  }

    // Support multiple agents on the same host.
    // =========================================
    // The -a host[@agent_port] option extends the agent
    // connection string to allow multiple agents running
    // on the same host with different ports. When agent_port
    // is part of agent option, the agent_port option is ignored.
    // It is still desireable to use agent_port option in the agent mode
    // when starting agent to listen on a specific zmq port.

    string aname=args.agent_arg[i];
    string host;

    size_t port_pos = aname.find("@");

    if (port_pos != string::npos) {
      string hstr = aname.substr(0, port_pos);
      string ip_addr = name_to_ipaddr(hstr, 0);
      string port = aname.substr(port_pos + 1, aname.length());
      host = string("tcp://") + ip_addr + string(":") + port;
    } else {
      host = string("tcp://") + name_to_ipaddr(args.agent_arg[i],0) +
        string(":") + string(args.agent_port_arg);
    }
		D("Add %s as agent\n",host.c_str());
		// setup socket to handle as many connections as we will need
    int nconns = args.measure_connections_given ? args.measure_connections_arg :
      options.connections;
    int nthreads = args.threads_given ? args.threads_arg :
      options.threads;
		
	int total_conn = 2 * (nconns+1) * (nthreads+1);
	if (total_conn<100)
		total_conn=100;
		
	s->setsockopt(ZMQ_BACKLOG,&total_conn,sizeof(total_conn));
	int linger=10000;
	s->setsockopt(ZMQ_LINGER,&linger,sizeof(linger));
	
	//then connect
	try {
		s->connect(host.c_str());
		agent_sockets.push_back(s);
	} catch (...) {
		DIE("Agent not available at %s!  Please make sure that the agent process is running, and the ports are open.\n",host.c_str());
	}

    //s->connect(host.c_str());
    //agent_sockets.push_back(s);
    }
  }
#endif


  pthread_barrier_init(&barrier, NULL, options.threads);
  
  cpu_info_t cpustat;
  pthread_create(&(cpustat.tid), 0, cpu_stat_thread, &cpustat);

  vector<string> servers;
  for (unsigned int s = 0; s < args.server_given; s++) {
	  string sname=args.server_arg[s];
	  size_t port_pos=sname.find("@");
	  if ((port_pos != string::npos) && 
		  (sname.find("-", port_pos) !=  string::npos)) { //parse range of ports on machine in case more then one instance is running
		  char *saveptr = NULL;  // For reentrant strtok().
		  char *cname=strdup(sname.c_str());

		  char *h_ptr = strtok_r(cname, "@", &saveptr);
		  char *start_port = strtok_r(NULL, "-", &saveptr);
		  char *end_port = strtok_r(NULL, "-", &saveptr);
		  
		  string ip_addr=name_to_ipaddr(h_ptr,0);
		  int sp=atoi(start_port);
		  int ep=atoi(end_port);
		  while (sp <= ep) {
			  char buf[48];
			  string s_instance=ip_addr + ":" + string( std::to_string(sp));
			  servers.push_back(s_instance);
			  sp++;
		  }
	  } else {
		servers.push_back(name_to_ipaddr(string(args.server_arg[s])));
	  }
  }

  ConnectionStats stats;
  if (args.plot_all_given)
	stats.plotall=true;

  double peak_qps = 0.0;
  bool avgseek=false;

  if (args.search_given) {
    char *n_ptr = strtok(args.search_arg, ":");
    char *x_ptr = strtok(NULL, ":");

    if (n_ptr == NULL || x_ptr == NULL) DIE("Invalid --search argument");

    if (strstr("avg",n_ptr)!=NULL) {
	avgseek=true;
    }
    int n = atoi(n_ptr);
    int x = atoi(x_ptr);

    if (avgseek) I("Search-mode.  Find QPS @ %dus avg latency.", x, n);
    else I("Search-mode.  Find QPS @ %dus %dth percentile.", x, n);

	//first determine max qps without paying attention to latency. 
    int high_qps = 10000000;
    int low_qps = 1; // 5000;
    double nth;
    int cur_qps;

    go(servers, options, stats);

    if (avgseek) nth=stats.get_avg();
    else nth = stats.get_nth(n);
    peak_qps = stats.get_qps();
    high_qps = stats.get_qps();
    cur_qps = stats.get_qps();

    I("peak qps = % 8d, %s = %.1f", high_qps, n_ptr, nth);
	//if latency at peak more then requested, search for requested latency setting to middle point between current and a point with known good latency
	//continue searching until the boundary between known good and current is ~5%

    if (nth > x) {
    while ((high_qps > low_qps * 1.02) && cur_qps > (peak_qps * .01)) {
      cur_qps = (high_qps + low_qps) / 2;

      args_to_options(&options);

      options.qps = cur_qps;
      options.lambda = (double) options.qps / (double) options.lambda_denom * args.lambda_mul_arg;

      stats = ConnectionStats();

      go(servers, options, stats);

      if (avgseek) nth=stats.get_avg();
      else nth = stats.get_nth(n);

      I(". target = % 8d, %s = %.1f, high_qps = %d, low_qps = %d, qps = %.0f", cur_qps,n_ptr,nth, high_qps, low_qps, stats.get_qps());

      if (nth > x /*|| cur_qps > stats.get_qps() * 1.05*/) high_qps = cur_qps;
      else low_qps = cur_qps;
    }

	// now if last value found at conversion has latency over the limit, use the last value before latency came within bounds, 
	// and decrease requested qps by 1% until latency comes within bounds, or we go below 90% of previously found value.
    while (nth > x && cur_qps > (peak_qps * .01) && cur_qps > (low_qps * 0.90)) {
      cur_qps = cur_qps * 99 / 100;

      args_to_options(&options);

      options.qps = cur_qps;
      options.lambda = (double) options.qps / (double) options.lambda_denom * args.lambda_mul_arg;

      stats = ConnectionStats();

      go(servers, options, stats);

      if (avgseek) nth=stats.get_avg();
      else nth = stats.get_nth(n);

      I(". target = % 8d, %s = %.1f, high_qps = %d, low_qps = %d, qps = %.0f", cur_qps,n_ptr,nth, high_qps, low_qps, stats.get_qps());
    }

    }
  } else if (args.scan_given) {
    char *min_ptr = strtok(args.scan_arg, ":");
    char *max_ptr = strtok(NULL, ":");
    char *step_ptr = strtok(NULL, ":");

    if (min_ptr == NULL || min_ptr == NULL || step_ptr == NULL)
      DIE("Invalid --scan argument");

    int min = atoi(min_ptr);
    int max = atoi(max_ptr);
    int step = atoi(step_ptr);
	
	stats.print_header(false);
    printf("%8s %8s\n", "QPS", "target");

    for (int q = min; q <= max; q += step) {
      args_to_options(&options);

      options.qps = q;
      options.lambda = (double) options.qps / (double) options.lambda_denom * args.lambda_mul_arg;

      	stats = ConnectionStats();
	reset_cpu_stats();
      	go(servers, options, stats);
	D("CPU Usage Stats (avg/min/max): %.2Lf%%,%.2Lf%%,%.2Lf%%\n",cpustat.avg,cpustat.min,cpustat.max);

      	stats.print_stats("read", stats.get_sampler, false);
      	printf(" %8.1f", stats.get_qps());
      	printf(" %8d\n", q);
    }    
  } else {
    go(servers, options, stats);
  }

  if (!args.scan_given && !args.loadonly_given) {
    stats.print_header();
    stats.print_stats("read",   stats.get_sampler, true, true);
    stats.print_stats("update", stats.set_sampler);
    stats.print_stats("op_q",   stats.op_sampler);

    float total = (float)(stats.gets + stats.sets);

    printf("\nTotal QPS = %.1f (%.0f / %.1fs)\n",
           total / (stats.stop - stats.start),
           total, stats.stop - stats.start);

    if (args.search_given && peak_qps > 0.0)
      printf("Peak QPS  = %.1f\n", peak_qps);

    printf("\n");
	
	printf("Total connections = %d\n", options.connections * options.server_given * options.threads);

    printf("Misses = %" PRIu64 " (%.1f%%)\n", stats.get_misses,
           (double) stats.get_misses/stats.gets*100);

    printf("Skipped TXs = %" PRIu64 " (%.1f%%)\n\n", stats.skips,
           (double) stats.skips / total * 100);

    printf("RX %10" PRIu64 " bytes : %6.1f MB/s\n",
           stats.rx_bytes,
           (double) stats.rx_bytes / 1024 / 1024 / (stats.stop - stats.start));
    printf("TX %10" PRIu64 " bytes : %6.1f MB/s\n",
           stats.tx_bytes,
           (double) stats.tx_bytes / 1024 / 1024 / (stats.stop - stats.start));

    if (args.save_given) {
      printf("Saving latency samples to %s.\n", args.save_arg);

      FILE *file;
      if ((file = fopen(args.save_arg, "w")) == NULL)
        DIE("--save: failed to open %s: %s", args.save_arg, strerror(errno));
	std::vector<Operation>::const_iterator i;
      for ( i= stats.get_sampler.samples.begin(); i!=stats.get_sampler.samples.end(); i++) {
        fprintf(file, "%f %f\n", i->start_time - boot_time, i->time());
      }
    }
  }

  stop_cpu_stats();
  pthread_join(cpustat.tid,0);
  if (!args.loadonly_given)
	printf("CPU Usage Stats (avg/min/max): %.2Lf%%,%.2Lf%%,%.2Lf%%\n",cpustat.avg,cpustat.min,cpustat.max);

  //  if (args.threads_arg > 1) 
    pthread_barrier_destroy(&barrier);

#ifdef HAVE_LIBZMQ
  if (args.agent_given) {
   vector<zmq::socket_t*>::iterator its;
   for ( its=agent_sockets.begin(); its!=agent_sockets.end(); its++ ) {
    zmq::socket_t *s=*its;
    //for (auto i: agent_sockets) delete i;
    delete s;
   }
  }
#endif

  // evdns_base_free(evdns, 0);
  // event_base_free(base);

  cmdline_parser_free(&args);
  return 0;
}

void go(const vector<string>& servers, options_t& options,
        ConnectionStats &stats
#ifdef HAVE_LIBZMQ
, zmq::socket_t* socket
#endif
) {
#ifdef HAVE_LIBZMQ
  if (args.agent_given > 0) {
V("agent given");
    prep_agent(servers, options);
V("Agent prep done.");
  }
#endif

  if (options.threads > 1) {
    pthread_t pt[options.threads];
    struct thread_data td[options.threads];
#ifdef __clang__
    vector<string>* ts = static_cast<vector<string>*>(alloca(sizeof(vector<string>) * options.threads));
#else
    vector<string> ts[options.threads];
#endif

    int current_cpu = -1;
    //options.qps/=options.threads;
    //options.lambda/=(double)options.threads;
D("Starting %d threads.", options.threads);
    for (int t = 0; t < options.threads; t++) {
      td[t].options = &options;

#ifdef HAVE_LIBZMQ
      td[t].socket = socket;
#endif
      if (t == 0) td[t].master = true;
      else td[t].master = false;

      if (options.roundrobin) {
        for (unsigned int i = (t % servers.size());
             i < servers.size(); i += options.threads)
          ts[t].push_back(servers[i % servers.size()]);

        td[t].servers = &ts[t];
      } else {
        td[t].servers = &servers;
      }

      pthread_attr_t attr;
      pthread_attr_init(&attr);

      if (args.affinity_given) {
        int max_cpus = 8 * sizeof(cpu_set_t);
        cpu_set_t m;
        CPU_ZERO(&m);
        sched_getaffinity(0, sizeof(cpu_set_t), &m);

        for (int i = 0; i < max_cpus; i++) {
          int c = (current_cpu + i + 1) % max_cpus;
          if (CPU_ISSET(c, &m)) {
            CPU_ZERO(&m);
            CPU_SET(c, &m);
            int ret;
            if ((ret = pthread_attr_setaffinity_np(&attr,
                                                   sizeof(cpu_set_t), &m)))
              DIE("pthread_attr_setaffinity_np(%d) failed: %s",
                  c, strerror(ret));
            current_cpu = c;
            break;
          }
        }
      }

      if (pthread_create(&pt[t], &attr, thread_main, &td[t]))
        DIE("pthread_create() failed");
    }
D("Fired all threads.");

    for (int t = 0; t < options.threads; t++) {
      ConnectionStats *cs;
D("Waiting for thread %d.",t);
      if (pthread_join(pt[t], (void**) &cs)) DIE("pthread_join() failed");
      stats.accumulate(*cs);
      delete cs;
    }
  } else if (options.threads == 1) {
    do_mcperf(servers, options, stats, true
#ifdef HAVE_LIBZMQ
, socket
#endif
);
  } else {
#ifdef HAVE_LIBZMQ
    if (args.agent_given) {
      int err=sync_agent(socket);
	if (err>0) DIE("ERRORS in agent sync!");
    }
#endif
  }

#ifdef HAVE_LIBZMQ
	if (args.agent_given || args.agentmode_given) {
    	float total = (float)(stats.gets) + (float)stats.sets;

	    V("Local QPS = %.1f (%lld / %.1fs)",
    	total / (stats.stop - stats.start),
      	total, stats.stop - stats.start);    
	}
	if (args.agent_given > 0) {
		finish_agent(stats);
	}
#endif
D("End of go()");
}

void* thread_main(void *arg) {
  struct thread_data *td = (struct thread_data *) arg;

  ConnectionStats *cs = new ConnectionStats();

  do_mcperf(*td->servers, *td->options, *cs, td->master
#ifdef HAVE_LIBZMQ
, td->socket
#endif
);

  return cs;
}

void do_mcperf(const vector<string>& servers, options_t& options,
                 ConnectionStats& stats, bool master
#ifdef HAVE_LIBZMQ
, zmq::socket_t* socket
#endif
) {
  int loop_flag =
    (options.blocking || args.blocking_given) ? EVLOOP_ONCE : EVLOOP_NONBLOCK;

  char *saveptr = NULL;  // For reentrant strtok().

  struct event_base *base;
  struct evdns_base *evdns;
  struct event_config *config;

  if ((config = event_config_new()) == NULL) DIE("event_config_new() fail");

#ifdef HAVE_DECL_EVENT_BASE_FLAG_PRECISE_TIMER
  if (event_config_set_flag(config, EVENT_BASE_FLAG_PRECISE_TIMER))
    DIE("event_config_set_flag(EVENT_BASE_FLAG_PRECISE_TIMER) fail");
#endif

  if ((base = event_base_new_with_config(config)) == NULL)
    DIE("event_base_new() fail");

  //  evthread_use_pthreads();

  if ((evdns = evdns_base_new(base, 1)) == 0) DIE("evdns");

  //  event_base_priority_init(base, 2);

  // FIXME: May want to move this to after all connections established.
  double start = get_time();
  double now = start;

  vector<Connection*> connections;
  vector<Connection*> server_lead;
	 vector<string>::const_iterator s;

  for (s=servers.begin(); s!=servers.end(); s++) {
    // Split args.server_arg[s] into host:port using strtok().
    char *s_copy = new char[s->length() + 1];
    strcpy(s_copy, s->c_str());

    char *h_ptr = strtok_r(s_copy, "@", &saveptr);
    char *p_ptr = strtok_r(NULL, "@", &saveptr);

    if (h_ptr == NULL) DIE("strtok(.., \":\") failed to parse %s", s->c_str());

    string hostname = h_ptr;
    string port = "11211";
    if (p_ptr) port = p_ptr;

    delete[] s_copy;

    int conns = args.measure_connections_given ? args.measure_connections_arg :
      options.connections;
	D("Connections req %s %d [%d/%d]",s->c_str(),conns,args.measure_connections_arg,options.connections);

    for (int c = 0; c < conns; c++) {

      Connection* conn = new Connection(base, evdns, hostname, port, options,
                                        args.agentmode_given ? true :
                                        true,
										args.keycache_capacity_given ? args.keycache_capacity_arg : 0,
										args.keycache_reuse_given ? args.keycache_reuse_arg : 0,
										args.keycache_regen_given ? args.keycache_regen_arg : 0);
      connections.push_back(conn);
      if (c == 0) server_lead.push_back(conn);
    }
  }

  // Wait for all Connections to become IDLE.
  int lcntr=0;
  struct timeval delay;
  //SG setup auto exit from evloop to break potential deadlock
  delay.tv_sec = 4;
  delay.tv_usec = 0;

  D("evt based loop start\n");
  while (1) {
    // FIXME: If all connections become ready before event_base_loop
    // is called, this will deadlock.
    event_base_loopexit(base, &delay);
    event_base_loop(base, EVLOOP_ONCE);

    bool restart = false;
	 vector<Connection*>::iterator conn;
	lcntr++;
	int cid=0;
    for (conn= connections.begin(); conn!=connections.end(); conn++ ) {
		if ((*conn)->read_state != Connection::IDLE) {
			restart = true;
		}
		cid++;
		if ((lcntr & 0x3f) == 0) {
			V("evt based loop [%d] taking long time. read state=%d/%d",lcntr,(*conn)->read_state,cid);
		}
	}
    if (restart) continue;
    else break;
  }
  D("evt based loop end\n");

  // Load database on lead connection for each server.
  // TODO: parallelize the database loading to multiple threads
  if (!options.noload) {
    D("Loading database.");
	vector<Connection*>::iterator c;
    for (c= server_lead.begin(); c!=server_lead.end(); c++ ) (*c)->start_loading();

    // Wait for all Connections to become IDLE.
    while (1) {
      // FIXME: If all connections become ready before event_base_loop
      // is called, this will deadlock.
    event_base_loopexit(base, &delay);
      event_base_loop(base, EVLOOP_ONCE );

      bool restart = false;
         vector<Connection*>::iterator conn;
    for (conn= connections.begin(); conn!=connections.end(); conn++ )
      if ((*conn)->read_state != Connection::IDLE) {
        restart = true;

	}

      if (restart) continue;
      else break;
    }
  }

  if (options.loadonly) {
    evdns_base_free(evdns, 0);
    event_base_free(base);
    return;
  }

  // FIXME: Remove.  Not needed, testing only.
  //  // FIXME: Synchronize start_time here across threads/nodes.
  //  pthread_barrier_wait(&barrier);

  // Warmup connection.
  if (options.warmup > 0) {
    if (master) V("Warmup start.");

#ifdef HAVE_LIBZMQ
    if (args.agent_given || args.agentmode_given) {
      if (master) V("Synchronizing.");

      // 1. thread barrier: make sure our threads ready before syncing agents
      // 2. sync agents: all threads across all agents are now ready
      // 3. thread barrier: don't release our threads until all agents ready
      int err=0;
      pthread_barrier_wait(&barrier);
      if (master) err=sync_agent(socket);
      pthread_barrier_wait(&barrier);

      if (master) V("Synchronized.");
	if (err>0) DIE("ERROR during synchronization! %s:%d",__FILE__,__LINE__);
    }
#endif

    int old_time = options.time;
    //    options.time = 1;

    start = get_time();
         vector<Connection*>::iterator iconn;
    for (iconn= connections.begin(); iconn!=connections.end(); iconn++ ) {
	Connection *conn=*iconn;

      conn->start_time = start;
      conn->options.time = options.warmup;
      conn->drive_write_machine(); // Kick the Connection into motion.
    }

    while (1) {
      event_base_loop(base, loop_flag);

      //#ifdef USE_CLOCK_GETTIME
      //      now = get_time();
      //#else
      struct timeval now_tv;
      event_base_gettimeofday_cached(base, &now_tv);
      now = tv_to_double(&now_tv);
      //#endif

      bool restart = false;
         vector<Connection*>::iterator iconn;
    for (iconn= connections.begin(); iconn!=connections.end(); iconn++ ) {
        Connection *conn=*iconn;
        if (!conn->check_exit_condition(now))
          restart = true;
	}

      if (restart) continue;
      else break;
    }

    bool restart = false;
    for (iconn= connections.begin(); iconn!=connections.end(); iconn++ ) {
        Connection *conn=*iconn;
      if (conn->read_state != Connection::IDLE)
        restart = true;
	}

    if (restart) {

    // Wait for all Connections to become IDLE.
		while (1) {
		  // FIXME: If there were to use EVLOOP_ONCE and all connections
		  // become ready before event_base_loop is called, this will
		  // deadlock.  We should check for IDLE before calling
		  // event_base_loop.
			event_base_loop(base, EVLOOP_ONCE); // EVLOOP_NONBLOCK);

			bool restart = false;
			vector<Connection*>::iterator iconn;
			for (iconn= connections.begin(); iconn!=connections.end(); iconn++ ) {
				Connection *conn=*iconn;
				if (conn->read_state != Connection::IDLE)
				  restart = true;
			}

			if (restart) continue;
			else break;
		}
    }

    //    options.time = old_time;
    for (iconn= connections.begin(); iconn!=connections.end(); iconn++ ) {
        Connection *conn=*iconn;
      conn->reset();
      //      conn->stats = ConnectionStats();
      conn->options.time = old_time;
    }

    if (master) V("Warmup stop.");
  }


  // FIXME: Synchronize start_time here across threads/nodes.
  pthread_barrier_wait(&barrier);

  if (master && args.wait_given) {
    if (get_time() < boot_time + args.wait_arg) {
      double t = (boot_time + args.wait_arg)-get_time();
      V("Sleeping %.1fs for -W.", t);
      sleep_time(t);
    }
  }

#ifdef HAVE_LIBZMQ
  if (args.agent_given || args.agentmode_given) {
    if (master) V("Synchronizing.");

	int err=0;
    pthread_barrier_wait(&barrier);
    if (master) err=sync_agent(socket);
    pthread_barrier_wait(&barrier);

    if (master) V("Synchronized.");
	if (err>0) DIE("ERROR during synchronization! %s:%d",__FILE__,__LINE__);
  }
#endif

  if (master && !args.scan_given && !args.search_given)
    V("started at %f", get_time());

	start = get_time();
	if (args.trace_given) { 
	/* 	To support tracing/simulation, in trace mode, 
		send special start_trace/stop_trace commands to the server,
		and at end of test, kill the server.
	*/
        Connection *conn=*connections.begin();
		conn->issue_command(command_string[0]);
	}
         vector<Connection*>::iterator iconn;
    for (iconn= connections.begin(); iconn!=connections.end(); iconn++ ) {
        Connection *conn=*iconn;
    conn->start_time = start;
    conn->drive_write_machine(); // Kick the Connection into motion.
  }

  //  V("Start = %f", start);

  // Main event loop.
  while (1) {
    event_base_loop(base, loop_flag);

    //#if USE_CLOCK_GETTIME
    //    now = get_time();
    //#else
    struct timeval now_tv;
    event_base_gettimeofday_cached(base, &now_tv);
    now = tv_to_double(&now_tv);
    //#endif

    bool restart = false;
         vector<Connection*>::iterator iconn;
    for (iconn= connections.begin(); iconn!=connections.end(); iconn++ ) {
        Connection *conn=*iconn;
      if (!conn->check_exit_condition(now))
        restart = true;
	}

    if (restart) continue;
    else break;
  }

  if (master && !args.scan_given && !args.search_given)
	if (args.trace_given) { 
	/* 	To support tracing/simulation, in trace mode, 
		send special start_trace/stop_trace commands to the server,
		and at end of test, kill the server.
	*/
        Connection *conn=*connections.begin();
		conn->issue_command(command_string[1]);
		conn->issue_command(command_string[2]);
	}
    V("stopped at %f  options.time = %d", get_time(), options.time);

  // Tear-down and accumulate stats.
	for (iconn= connections.begin(); iconn!=connections.end(); iconn++ ) {
		Connection *conn=*iconn;
		stats.accumulate(conn->stats);
		delete conn;
	}

	stats.start = start;
	stats.stop = now;

	event_config_free(config);
	evdns_base_free(evdns, 0);
	event_base_free(base);
}

typedef struct mc_profile_s {
	float min_c;
	int r_size;
	char const *search;
	char const *ks;
	char const *kg;
	char const *v;
	char const *ia;
	int qps;
	int cc,cr,cg; // key cache parameters
	int depth;
	float update_ratio;
} mc_profile;

static mc_profile mc_profiles[] = {
//1. memcached for web serving benchmark : p95, 20ms, FB key/value/IA, >4000 connections to the device under test.
	{4000.,1000000,"95:20000","fb_key","none","fb_value","fb_ia",0,10000,20,1,0,0},
//2. memcached for applications backends : p99, 10ms, 32B key , 1000B value, uniform IA,  >1000 connections
	{1000.,1000000,"99:10000","32","none","1000",NULL,0,10000,10,1,0,0},
//3. memcached for low latency (e.g. stock trading): report p99.9, 32B key, 200B value, uniform IA, QPS rate set to 100000	
	{1.,1000000,NULL,"32","none","200",NULL,100000,10000,5,1,0,0},
//4. P99.9, 1 msec. Key size = 32 bytes; value size has uniform distribution from 100 bytes to 1k; 
//	 key request should arrive with zipfian distribution; metric is QPS.
//	Instead of zipf, use pareto distribution with scale of 16 and shape of 0.154971, 0 offset.
	{100.,1000000,"999:1000","32","pareto:0.0,16,0.154971","uniform:100,1000",NULL,0,10000,10,1,0,0},
//5. Update heavy large queue memcached
	{5.,58753,NULL,"114","none","12518",NULL,0,10000,20,1,148,0.623595},
//6. Update heavy high latency memcached
	{1.,371510,NULL,"243","none","15578",NULL,0,10000,20,1,249,0.558257},
//7. Small DB low connection memcached
	{6.,1000,NULL,"250","none","102720",NULL,0,10000,20,1,207,0},
//8. Small DB low connection large queue memcached
	{7.,1000,NULL,"250","none","139851",NULL,0,10000,20,1,250,0},
//9. Small DB low connection large queue with updates memcached
	{7.,1000,NULL,"250","none","139851",NULL,0,10000,20,1,250,0.001114},
};
#define max_profiles (sizeof(mc_profiles)/sizeof(mc_profile))

void profile_update_dist(char **arg, const char *update, unsigned int *given) {
	if (update != NULL) {
		*arg=strdup(update);
		*given=1;
	}
}

void profile_update_dist(int *arg, const int update, unsigned int *given) {
	if (update != 0) {
		*arg=update;
		*given=1;
	}
}

void parse_profile() {
  if (args.profile_given) {
	int profile=args.profile_arg;
	if (profile>max_profiles || profile<1) {
		V("Unrecognized Profile! Using profile 1 instead");
		profile=1;
	} 
	mc_profile *p=&mc_profiles[profile-1];
	//setup connections
	int total_threads=args.server_given * args.threads_arg; 
	int connections_per_thread=ceil(p->min_c/(float)total_threads); //assumes all servers are running on the same DUT
	if (args.connections_arg < connections_per_thread)
		args.connections_arg=connections_per_thread;
	//setup size
	args.records_arg=p->r_size;
	//setup mode
	if (p->search != NULL) {
		args.search_given=1;
		args.search_arg=strdup(p->search);
	}
	//set distributions
	profile_update_dist(&args.keysize_arg,p->ks,&args.keysize_given);
	profile_update_dist(&args.valuesize_arg,p->v,&args.valuesize_given);
	profile_update_dist(&args.iadist_arg,p->ia,&args.iadist_given);
	profile_update_dist(&args.keyorder_arg,p->kg,&args.keyorder_given);
	profile_update_dist(&args.keycache_capacity_arg,p->cc,&args.keycache_capacity_given);
	profile_update_dist(&args.keycache_reuse_arg,p->cc,&args.keycache_reuse_given);
	profile_update_dist(&args.keycache_regen_arg,p->cc,&args.keycache_regen_given);
	if (p->qps > 0) {
		args.qps_arg = p->qps;
	}
	if (p->depth > 0) {
		args.depth_arg = p->depth;
	}
	if (p->update_ratio >= 0) {
		args.update_arg = p->update_ratio;
	}
  }
}

void args_to_options(options_t* options) {
  parse_profile();
  options->connections = args.connections_arg;
  options->blocking = args.blocking_given;
  options->qps = args.qps_arg;
  options->threads = args.threads_arg;
  options->server_given = args.server_given;
  options->roundrobin = args.roundrobin_given;

  //actual connections are connections per thread * number of threads
  //allocation of connections via lambda
  int connections = options->connections;
  if (options->roundrobin) {
    connections *= (options->server_given > options->threads ?
                    options->server_given : options->threads);
  } else {
    connections *= options->server_given * options->threads;
  }

  //  if (args.agent_given) connections *= (1 + args.agent_given);

  options->lambda_denom = connections > 1 ? connections : 1;
  if (args.lambda_mul_arg > 1) options->lambda_denom *= args.lambda_mul_arg;

  if (options->threads < 1) options->lambda_denom = 0;

  options->lambda = (double) options->qps / (double) options->lambda_denom * args.lambda_mul_arg;

  D("%d %d %d %f", options->qps, options->connections,connections, options->lambda);

  if (options->server_given==0)
	options->server_given=1;
  options->records = args.records_arg / options->server_given;

  options->binary = args.binary_given;
  options->sasl = args.username_given;
  
  if (args.password_given)
    strcpy(options->password, args.password_arg);
  else
    strcpy(options->password, "");

  if (args.username_given)
    strcpy(options->username, args.username_arg);
  else
    strcpy(options->username, "");

  D("options->records = %d", options->records);

  if (!options->records) options->records = 1;
  strcpy(options->keysize, args.keysize_arg);
  strcpy(options->keyorder, args.keyorder_arg);
  strcpy(options->valuesize, args.valuesize_arg);
  options->update = args.update_arg;
  options->time = args.time_arg;
  options->loadonly = args.loadonly_given;
  options->depth = args.depth_arg;
  options->no_nodelay = args.no_nodelay_given;
  options->noload = args.noload_given;
  options->iadist = get_distribution(args.iadist_arg);
  strcpy(options->ia, args.iadist_arg);
  options->warmup = args.warmup_given ? args.warmup_arg : 0;
  options->oob_thread = false;
  options->skip = args.skip_given;
  options->moderate = args.moderate_given;
  options->getq_freq = args.getq_freq_given ? args.getq_freq_arg : 0.0;
  options->getq_size = args.getq_size_arg;
}

void init_random_stuff() {
  static char lorem[] =
    R"(Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas
turpis dui, suscipit non vehicula non, malesuada id sem. Phasellus
suscipit nisl ut dui consectetur ultrices tincidunt eros
aliquet. Donec feugiat lectus sed nibh ultrices ultrices. Vestibulum
ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia
Curae; Mauris suscipit eros sed justo lobortis at ultrices lacus
molestie. Duis in diam mi. Cum sociis natoque penatibus et magnis dis
parturient montes, nascetur ridiculus mus. Ut cursus viverra
sagittis. Vivamus non facilisis tortor. Integer lectus arcu, sagittis
et eleifend rutrum, condimentum eget sem. Vestibulum tempus tellus non
risus semper semper. Morbi molestie rhoncus mi, in egestas dui
facilisis et.)";

  size_t cursor = 0;

  while (cursor < sizeof(random_char)) {
    size_t max = sizeof(lorem);
    if (sizeof(random_char) - cursor < max)
      max = sizeof(random_char) - cursor;

    memcpy(&random_char[cursor], lorem, max);
    cursor += max;
  }
}

